#!/usr/bin/env python3
"""
Automated traffic data collection script for GitHub Actions
Queries the active region from collection_config and collects data if interval has passed
"""

import os       #to read the environment variables containing our keys (on github)
import sys      #lets python talk to python interpreter directly - we use it to stop our program
from datetime import datetime, timedelta    #to get curr date-time; write a time difference quantity 
import psycopg2     #PGSQL DB connector for python
from psycopg2.extras import execute_values      #helper function to insert many rows at once
import requests     #module to make http requests - here to our API
import time        #to get time related stuff and keep track of execution time

#Class that does the collection work

class ScheduledTrafficCollector:
    def __init__(self, db_conn_string, api_key):

        #db_conn_string is the NeonDB string that allows us to connect to the cloud based DB
        #api_key is Google's Distance Matrix API key

        self.conn = psycopg2.connect(db_conn_string)        #initialize the connection for the entire class
        self.api_key = api_key
        self.DAILY_LIMIT = 1100  #1,129 elements/day worst case but we end before to be safe

    
    def should_query_now(self, last_queried_at, interval_hours):
        """Check if enough time has passed since last query"""

        if last_queried_at is None:
            #edge case a region first time being queried, last_queried_at is None
            return True
        
        #else check if we passed the interval already, if yes return True (should query now) else False (don't query)
        now = datetime.utcnow()
        time_since_last = (now - last_queried_at).total_seconds() / 3600
        
        return time_since_last >= interval_hours
    
    
    def get_active_region(self):
        """Get the currently active region"""

        cursor = self.conn.cursor()         #cursor() allows us to execute SQL queries in our connected DB
        
        #index has been created on the is_active column in collection_config table for fast check
        cursor.execute("""
            SELECT 
                cc.region_id,
                r.region_name,
                cc.queries_per_day,
                cc.interval_hours,
                cc.last_queried_at,
                r.segment_count
            FROM collection_config cc
            JOIN regions r ON cc.region_id = r.region_id
            WHERE cc.is_active = TRUE
            LIMIT 1
        """)
        
        result = cursor.fetchone()      #gets the first row of the data retrieved (there will be one only as we limit to 1)
        cursor.close()
        
        return result
    
    def collect_traffic_data(self, region_id):
        """Collect traffic data for all segments in a region given the region_id"""
        cursor = self.conn.cursor()
        
        print(f"Collecting traffic data for region {region_id}...")
        
        # Get all segments with their start and end lat, long for this region
        cursor.execute("""
            SELECT segment_id, start_lat, start_lng, end_lat, end_lng 
            FROM road_segments 
            WHERE region_id = %s
        """, (region_id,))
        
        # get all the rows that come up
        segments = cursor.fetchall()
        print(f"Querying {len(segments)} segments...")
        
        timestamp = datetime.utcnow()
        # day_of_week = timestamp.weekday()
        # hour_of_day = timestamp.hour
        # minute_of_hour = timestamp.minute
        
        observations = []       #for the traffic_observations table
        segment_updates = []    #for the query_log table
        successful = 0
        failed = 0
        
        start_time = time.time()
        
        for idx, (seg_id, start_lat, start_lng, end_lat, end_lng) in enumerate(segments):
            try:
                print(f"  [{idx+1}/{len(segments)}] Segment {seg_id}...", end=' ')
                
                #query the particular road segment, this function additionally returns success and failure numbers
                traffic_data = self._query_distance_matrix(
                    f"{start_lat},{start_lng}",
                    f"{end_lat},{end_lng}"
                )
                
                if traffic_data:

                    #observations to insert into traffic_observations
                    observations.append((
                        seg_id, region_id, timestamp,
                        #day_of_week, hour_of_day, minute_of_hour,
                        traffic_data['freeflow_duration'],
                        traffic_data['current_duration'],
                        #traffic_data['delay'],
                        #traffic_data['congestion_ratio'],
                        #traffic_data['current_speed'],
                        #traffic_data['freeflow_speed'],
                        #traffic_data['congestion_level']
                    ))
                    
                    segment_updates.append((traffic_data['distance'], seg_id))
                    
                    #print(f"✅ Ratio: {traffic_data['congestion_ratio']:.2f}")
                    successful += 1
                else:
                    print(f"Failed")
                    failed += 1
                
                time.sleep(0.2)  # Rate limiting
                
            except Exception as e:
                print(f"Error: {e}")
                failed += 1
        
        end_time = time.time()
        elapsed_time = int((end_time - start_time) * 1000)
        
        # Update road segment distances
        if segment_updates:
            cursor.executemany("""
                UPDATE road_segments 
                SET segment_distance_api = %s 
                WHERE segment_id = %s
            """, segment_updates)
        
        # Insert observations
        if observations:
            print(f"💾 Saving {len(observations)} observations...")
            execute_values(cursor, """
                INSERT INTO traffic_observations 
                (segment_id, region_id, observed_at,
                 freeflow_duration_seconds, current_duration_seconds)
                VALUES %s
            """, observations)
        
        # Log the query
        cursor.execute("""
            INSERT INTO query_log (region_id, elements_queried, successful_queries, failed_queries, api_response_time_ms)
            VALUES (%s, %s, %s, %s, %s)
        """, (region_id, len(segments), successful, failed, elapsed_time))
        
        self.conn.commit()
        cursor.close()
        
        return successful, failed
    
    def _query_distance_matrix(self, origin, destination):
        """Query Google Distance Matrix API"""
        url = "https://maps.googleapis.com/maps/api/distancematrix/json"
        params = {
            'origins': origin,
            'destinations': destination,
            'mode': 'driving',
            'departure_time': 'now',
            'traffic_model': 'best_guess',
            'key': self.api_key
        }
        
        try:
            response = requests.get(url, params=params, timeout=10)
            data = response.json()
            
            # Check for quota exceeded
            if data['status'] == 'OVER_QUERY_LIMIT':
                print("QUOTA EXCEEDED!")
                return None
            
            if data['status'] == 'OK':
                element = data['rows'][0]['elements'][0]
                
                if element['status'] == 'OVER_QUERY_LIMIT':
                    print("Element quota exceeded")
                    return None
                
                if element['status'] == 'OK':
                    duration = element.get('duration', {}).get('value')
                    duration_in_traffic = element.get('duration_in_traffic', {}).get('value')
                    distance = element.get('distance', {}).get('value')
                    
                    if duration and distance:
                        current_duration = duration_in_traffic if duration_in_traffic else duration
                        delay = current_duration - duration
                        #congestion_ratio = current_duration / duration
                        freeflow_speed = (distance / duration) * 3.6
                        current_speed = (distance / current_duration) * 3.6
                        
                        # if congestion_ratio <= 1.1:
                        #     congestion = 'free'
                        # elif congestion_ratio <= 1.3:
                        #     congestion = 'light'
                        # elif congestion_ratio <= 1.6:
                        #     congestion = 'moderate'
                        # elif congestion_ratio <= 2.0:
                        #     congestion = 'heavy'
                        # else:
                        #     congestion = 'severe'
                        
                        return {
                            'distance': distance,
                            'freeflow_duration': duration,
                            'current_duration': current_duration,
                            #'delay': delay,
                            #'congestion_ratio': round(congestion_ratio, 3),
                            #'current_speed': round(current_speed, 2),
                            #'freeflow_speed': round(freeflow_speed, 2),
                            #'congestion_level': congestion
                        }
        except Exception as e:
            print(f"API Error: {e}")
        
        return None
    
    def run(self):
        """Main function called by GitHub Actions"""
        print("=" * 60)
        print("SCHEDULED TRAFFIC DATA COLLECTION")
        print(f"Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
        print("=" * 60)
        
        # Get active region
        active = self.get_active_region()
        
        if not active:
            print("No active region configured.")
            print("Use start_collection(region_id) to activate a region")
            return
        
        region_id, name, queries_per_day, interval_hours, last_queried, segment_count = active
        
        print(f"\n Active Region: {name or f'Region {region_id}'}")
        print(f"   Region ID: {region_id}")
        print(f"   Segments: {segment_count}")
        print(f"   Target: {queries_per_day} queries/day")
        print(f"   Interval: Every {float(interval_hours):.2f} hours")
        
        if last_queried:
            hours_since = (datetime.utcnow() - last_queried).total_seconds() / 3600
            print(f"   Last queried: {last_queried.strftime('%Y-%m-%d %H:%M:%S')} UTC ({hours_since:.2f}h ago)")
        else:
            print(f"   Last queried: Never")
        
        # Check if we should query now
        if not self.should_query_now(last_queried, float(interval_hours)):
            hours_since = (datetime.utcnow() - last_queried).total_seconds() / 3600
            hours_remaining = float(interval_hours) - hours_since
            print(f"\n⏭️ SKIPPING: Too soon since last query")
            print(f"   Time since last: {hours_since:.2f}h")
            print(f"   Required interval: {float(interval_hours):.2f}h")
            print(f"   Time remaining: {hours_remaining:.2f}h")
            print("\n" + "=" * 80)
            return
        
        print(f"\n Interval passed. Starting collection...")
        print("-" * 60)
        
        # Collect data
        try:
            successful, failed = self.collect_traffic_data(region_id)
            
            # Update last queried time
            cursor = self.conn.cursor()
            cursor.execute("""
                UPDATE collection_config 
                SET last_queried_at = %s, updated_at = %s
                WHERE region_id = %s
            """, (datetime.utcnow(), datetime.utcnow(), region_id))
            self.conn.commit()
            cursor.close()
            
            print("-" * 80)
            print(f"\n✅ COLLECTION COMPLETE!")
            print(f"   Successful queries: {successful}")
            print(f"   Failed queries: {failed}")
            print(f"   Success rate: {(successful/(successful+failed)*100):.1f}%")
            
        except Exception as e:
            print(f"\n COLLECTION FAILED: {e}")
            raise
        
        finally:
            print("=" * 80)


if __name__ == "__main__":
    # Get credentials from environment variables
    DB_CONNECTION = os.environ.get('DB_CONNECTION_STRING')
    GOOGLE_API_KEY = os.environ.get('GOOGLE_API_KEY')
    
    if not DB_CONNECTION:
        print("Error: DB_CONNECTION_STRING environment variable not set")
        sys.exit(1)
    
    if not GOOGLE_API_KEY:
        print("Error: GOOGLE_API_KEY environment variable not set")
        sys.exit(1)
    
    # Run collection
    collector = ScheduledTrafficCollector(DB_CONNECTION, GOOGLE_API_KEY)
    collector.run()
