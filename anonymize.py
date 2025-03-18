import pandas as pd
import numpy as np
import hashlib
import random

# Load the data
# Assuming tab-separated values based on your sample
df = pd.read_csv('data/AllDataset.txt', sep='\t', header=None, encoding='cp1252')

# Define column names based on the structure observed
columns = [
    'patient_record_id', 'document_id', 'first_name', 'middle_name', 'last_name', 
    'id_type', 'id_number', 'location_code', 'sample_location', 'sample_type', 
    'test_code', 'test_name', 'parameter_name', 'service_type', 'department', 
    'sample_date', 'result_date', 'order_date'
]

# Apply column names
df.columns = columns

# Create a deterministic anonymization function
def anonymize_id(value, salt="your_secret_salt"):
    """Create a consistent hash for identifiers to maintain referential integrity"""
    return hashlib.sha256(f"{value}{salt}".encode()).hexdigest()[:12]

# Create anonymization mapping dictionaries to ensure consistency
patient_id_map = {}
document_id_map = {}
location_map = {}

# Function to process in chunks (for large datasets)
def process_chunk(chunk_df):

    for column in ['patient_record_id', 'document_id', 'id_number', 'first_name', 'middle_name', 'last_name', 'sample_location']:
        chunk_df[column] = chunk_df[column].astype('object')

    for index, row in chunk_df.iterrows():
        # Anonymize patient_record_id
        if row['patient_record_id'] not in patient_id_map:
            patient_id_map[row['patient_record_id']] = f"PT{anonymize_id(str(row['patient_record_id']))}"
        chunk_df.at[index, 'patient_record_id'] = patient_id_map[row['patient_record_id']]
        
        # Anonymize document_id
        if row['document_id'] not in document_id_map:
            document_id_map[row['document_id']] = f"DOC{anonymize_id(str(row['document_id']))}"
        chunk_df.at[index, 'document_id'] = document_id_map[row['document_id']]
        
        # Anonymize ID number
        chunk_df.at[index, 'id_number'] = f"ID{anonymize_id(str(row['id_number']))}"
        
        # Replace names with generic placeholders
        chunk_df.at[index, 'first_name'] = f"FIRSTNAME{hash(row['first_name']) % 1000}"
        chunk_df.at[index, 'middle_name'] = f"MIDDLENAME{hash(row['middle_name']) % 500}"
        chunk_df.at[index, 'last_name'] = f"LASTNAME{hash(row['last_name']) % 1000}"
        
        # Anonymize location
        if row['sample_location'] not in location_map:
            location_map[row['sample_location']] = f"LOCATION{len(location_map) + 1}"
        chunk_df.at[index, 'sample_location'] = location_map[row['sample_location']]
        
        # Dates remain unchanged
    
    return chunk_df

# Process the entire dataframe or in chunks based on size
if len(df) > 100000:
    # Process in chunks for large datasets
    result_chunks = []
    chunk_size = 100000
    for i in range(0, len(df), chunk_size):
        chunk = df.iloc[i:i+chunk_size].copy()
        result_chunks.append(process_chunk(chunk))
    
    # Combine processed chunks
    df_anonymized = pd.concat(result_chunks, ignore_index=True)
else:
    # Process small dataset at once
    df_anonymized = process_chunk(df.copy())

# Save the anonymized data
df_anonymized.to_csv('anonymized_data.csv', index=False)
print("Anonymization complete. Data saved to 'anonymized_data.csv'")

# Optional: Export the mapping dictionaries for future reference or reversal if needed
import json
with open('anonymization_mappings.json', 'w') as f:
    json.dump({
        'patient_ids': patient_id_map,
        'document_ids': document_id_map,
        'locations': location_map
    }, f)