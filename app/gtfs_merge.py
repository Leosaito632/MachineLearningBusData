import pandas as pd

# --- Step 1: Create a function to convert HH:MM:SS to seconds ---
def time_to_seconds(time_str):
    """Converts a HH:MM:SS string to total seconds from midnight."""
    try:
        h, m, s = map(int, time_str.split(':'))
        return h * 3600 + m * 60 + s
    except (ValueError, AttributeError):
        # Return NaN or another placeholder if the time is invalid or missing
        return None

# --- Step 2: Load your data ---
gtfs_pasta = 'app/datasets/gtfs_rio-de-janeiro/' # Replace with your actual path
stop_times = pd.read_csv(f'{gtfs_pasta}stop_times.txt')
trips = pd.read_csv(f'{gtfs_pasta}trips.txt')
# ... load other files as needed ...

# --- Step 3: Apply the conversion BEFORE calculations ---
# This is the crucial new step.
print("Converting time columns to seconds...")
stop_times['arrival_seconds'] = stop_times['arrival_time'].apply(time_to_seconds)
stop_times['departure_seconds'] = stop_times['departure_time'].apply(time_to_seconds)


# --- Step 4: Perform your merges ---
df = pd.merge(stop_times, trips, on='trip_id')
trips_rota = trips[trips['route_id'] == "O0371AAA0A"]
df = df.merge(trips_rota, on='trip_id')


# ... continue with other merges ...


# --- Step 5: Now, calculate the travel time using the NEW numeric column ---
print("Calculating travel time...")
# Sort values to ensure .diff() is calculated correctly along a trip's sequence
df = df.sort_values(by=['trip_id', 'stop_sequence'])

# Use the 'arrival_seconds' column for the calculation
df['travel_time_seconds'] = df.groupby('trip_id')['arrival_seconds'].diff()

# Display the result to verify
print(df.head(10))
df.to_csv('app/output/gtfs-rio-de-janeiro-merge-2.csv', index=False)