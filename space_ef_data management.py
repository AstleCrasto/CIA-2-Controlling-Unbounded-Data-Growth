import pandas as pd
import random
import csv
import os
import zlib
from datetime import datetime, timedelta

def generate_sample_data(filename, num_records):
    """
    Generates a sample CSV file with a mix of high-volume and low-volume data.
    """
    with open(filename, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['timestamp', 'device_id', 'event_type', 'payload'])
        
        start_time = datetime(2023, 1, 1, 0, 0, 0)
        
        for i in range(num_records):
            timestamp = start_time + timedelta(seconds=i * 0.1)
            device_id = random.randint(1, 100)
            
            # 90% high-volume 'ping' events
            if random.random() < 0.90:
                event_type = 'ping'
                payload = 'OK'
            # 10% low-volume, unique events
            else:
                event_type = random.choice(['critical_error', 'user_action'])
                payload = f"unique_event_{random.randint(1000, 9999)}"

            # Inject some duplicate unique records for deduplication test
            if i % 1000 == 0:
                payload = "duplicate_payload"
            
            writer.writerow([timestamp.isoformat(), f'dev_{device_id}', event_type, payload])

def run_dsop_pipeline(input_filename, output_filename, compressed_filename):
    """
    Runs the Data Stream Optimization Pipeline (DSOP) on the sample data.
    """
    print("--- DSOP Pipeline Execution ---")
    
    # --- Layer 1: Summarization ---
    print("\nLayer 1: Real-time Summarization and Filtering...")
    df = pd.read_csv(input_filename)
    
    # Filter out high-volume 'ping' events
    high_volume_df = df[df['event_type'] == 'ping'].copy()
    low_volume_df = df[df['event_type']!= 'ping'].copy()
    
    # Aggregate high-volume data
    high_volume_df['timestamp'] = pd.to_datetime(high_volume_df['timestamp'])
    high_volume_df['hour'] = high_volume_df['timestamp'].dt.to_period('H')
    aggregated_df = high_volume_df.groupby(['hour', 'device_id']).agg(
        total_pings=('event_type', 'count')
    ).reset_index()
    
    print(f"Original high-volume records: {len(high_volume_df)}")
    print(f"Aggregated records: {len(aggregated_df)}")
    
    # Combine aggregated data with low-volume data
    low_volume_df = low_volume_df.drop(columns=['timestamp'])
    
    final_df = pd.concat([aggregated_df, low_volume_df.rename(
        columns={'event_type': 'total_pings', 'payload': 'payload_content'}
    )], ignore_index=True)

    # --- Layer 2: Deduplication ---
    print("\nLayer 2: Deduplication...")
    initial_record_count = len(final_df)
    final_df.drop_duplicates(subset=['device_id', 'total_pings', 'payload_content'], inplace=True)
    deduplicated_record_count = len(final_df)
    
    print(f"Records before deduplication: {initial_record_count}")
    print(f"Records after deduplication: {deduplicated_record_count}")
    
    # Save the processed data
    final_df.to_csv(output_filename, index=False)
    
    # --- Layer 3: Compression ---
    print("\nLayer 3: Compression...")
    with open(output_filename, 'rb') as f_in:
        with open(compressed_filename, 'wb') as f_out:
            f_out.write(zlib.compress(f_in.read()))

# --- Main Execution ---
if __name__ == "__main__":
    input_file = "telemetry_raw.csv"
    output_file = "telemetry_processed.csv"
    compressed_file = "telemetry_processed.csv.zlib"
    
    # Generate the sample data
    num_records_to_generate = 100000
    generate_sample_data(input_file, num_records_to_generate)
    
    # Get baseline metrics
    initial_size_mb = os.path.getsize(input_file) / (1024 * 1024)
    print(f"Initial raw data size: {initial_size_mb:.2f} MB")
    print(f"Initial record count: {num_records_to_generate}")
    
    # Run the DSOP pipeline
    run_dsop_pipeline(input_file, output_file, compressed_file)
    
    # Get final metrics
    final_size_mb = os.path.getsize(compressed_file) / (1024 * 1024)
    final_record_count = len(pd.read_csv(output_file))
    
    print("\n--- Final Results ---")
    print(f"Final processed record count: {final_record_count}")
    print(f"Final compressed size: {final_size_mb:.2f} MB")
    
    space_reduction_ratio = (1 - (final_size_mb / initial_size_mb)) * 100
    print(f"Overall space reduction ratio: {space_reduction_ratio:.2f}%")