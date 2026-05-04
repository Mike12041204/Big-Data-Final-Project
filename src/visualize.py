<<<<<<< HEAD
"""
Visualization Script for Big Data Taxi Pipeline Results
Creates charts and graphs from MongoDB aggregated data and performance benchmarks
"""

import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError, ConnectionFailure
import os
import json
from datetime import datetime

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), '..', 'visualized_results')
os.makedirs(OUTPUT_DIR, exist_ok=True)
print(f"Saving visualizations to: {os.path.abspath(OUTPUT_DIR)}")

# Set up plotting style
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

def connect_mongodb():
    """Connect to MongoDB with fallback logic"""
    mongodb_uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017/?directConnection=true")
    docker_uri = os.getenv(
        "DOCKER_MONGODB_URI",
        "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0",
    )

    try:
        print(f"Connecting to MongoDB at {mongodb_uri}")
        client = MongoClient(mongodb_uri, serverSelectionTimeoutMS=5000, connectTimeoutMS=5000)
        client.admin.command('ping')
        print("Connected to MongoDB")
        return client
    except (ServerSelectionTimeoutError, ConnectionFailure):
        print("Local MongoDB not available, trying Docker replica set...")
        try:
            client = MongoClient(docker_uri, serverSelectionTimeoutMS=10000, connectTimeoutMS=10000)
            client.admin.command('ping')
            print("Connected to Docker MongoDB replica set")
            return client
        except (ServerSelectionTimeoutError, ConnectionFailure) as e:
            print(f"Failed to connect to MongoDB: {e}")
            print("Make sure MongoDB is running:")
            print("  - For Docker: Run '.\\run.ps1'")
            print("  - For local: Install MongoDB and start mongod service")
            raise

def load_aggregated_data(client):
    """Load aggregated summary data"""
    try:
        summary = client['aggregated']['summary'].find_one()
        if summary:
            print("Loaded aggregated summary data")
            return summary
        else:
            print("No aggregated summary data found. Run the pipeline first.")
            return None
    except Exception as e:
        print(f"Error loading aggregated data: {e}")
        return None

def load_performance_data(client):
    """Load performance benchmark data"""
    try:
        perf = client['raw']['performance_results'].find_one()
        if perf:
            print("Loaded performance benchmark data")
            return perf
        else:
            print("No performance data found. Run the pipeline first.")
            return None
    except Exception as e:
        print(f"Error loading performance data: {e}")
        return None

def plot_trips_by_hour(summary):
    """Plot trips distribution by pickup hour"""
    if 'trips_by_hour' not in summary or not summary['trips_by_hour']:
        print("No trips by hour data available")
        return

    hours_data = summary['trips_by_hour']
    df = pd.DataFrame(hours_data)

    plt.figure(figsize=(12, 6))
    plt.bar(df['pickup_hour'], df['count'], color='skyblue', alpha=0.7)
    plt.title('Taxi Trips by Pickup Hour', fontsize=16, fontweight='bold')
    plt.xlabel('Hour of Day (0-23)', fontsize=12)
    plt.ylabel('Number of Trips', fontsize=12)
    plt.xticks(range(0, 24, 2))
    plt.grid(axis='y', alpha=0.3)

    # Highlight peak hours
    peak_hour = df.loc[df['count'].idxmax()]
    plt.axvline(x=peak_hour['pickup_hour'], color='red', linestyle='--', alpha=0.7,
                label=f'Peak: {peak_hour["pickup_hour"]}:00 ({peak_hour["count"]:,} trips)')
    plt.legend()

    plt.tight_layout()
    output_path = os.path.join(OUTPUT_DIR, 'trips_by_hour.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()  # Use close() instead of show() to avoid GUI popups
    print(f"Saved: {output_path}")

def plot_top_locations(summary):
    """Plot top pickup locations"""
    if 'top_pickup_locations' not in summary or not summary['top_pickup_locations']:
        print("No top pickup locations data available")
        return

    locations_data = summary['top_pickup_locations']
    df = pd.DataFrame(locations_data)

    plt.figure(figsize=(10, 6))
    bars = plt.bar(range(len(df)), df['count'], color='lightgreen', alpha=0.7)
    plt.title('Top 5 Pickup Locations', fontsize=16, fontweight='bold')
    plt.xlabel('Location ID', fontsize=12)
    plt.ylabel('Number of Trips', fontsize=12)
    plt.xticks(range(len(df)), df['pickup_location_id'])
    plt.grid(axis='y', alpha=0.3)

    # Add value labels on bars
    for bar, count in zip(bars, df['count']):
        plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 50,
                f'{count:,}', ha='center', va='bottom', fontsize=10)

    output_path = os.path.join(OUTPUT_DIR, 'top_pickup_locations.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Saved: {output_path}")

def plot_payment_types(summary):
    """Plot payment type distribution"""
    if 'top_payment_types' not in summary or not summary['top_payment_types']:
        print("No payment types data available")
        return

    payment_data = summary['top_payment_types']
    df = pd.DataFrame(payment_data)

    plt.figure(figsize=(8, 6))
    plt.pie(df['count'], labels=df['payment_type'], autopct='%1.1f%%',
            startangle=90, colors=sns.color_palette("pastel"))
    plt.title('Payment Type Distribution', fontsize=16, fontweight='bold')
    plt.axis('equal')

    output_path = os.path.join(OUTPUT_DIR, 'payment_types.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Saved: {output_path}")

def plot_performance_benchmark(perf_data):
    """Plot query performance improvement"""
    if not perf_data:
        print("No performance data available")
        return

    # Extract timing data
    without_index = perf_data.get('metrics', {}).get('without_index_ms', 0)
    with_index = perf_data.get('metrics', {}).get('with_index_ms', 0)
    improvement = perf_data.get('metrics', {}).get('improvement_percent', 0)

    if without_index == 0 or with_index == 0:
        print("Incomplete performance data")
        return

    labels = ['Without Index', 'With Index']
    times = [without_index, with_index]

    plt.figure(figsize=(10, 6))

    # Bar chart
    bars = plt.bar(labels, times, color=['red', 'green'], alpha=0.7)
    plt.title('Query Performance: passenger_count Filter', fontsize=16, fontweight='bold')
    plt.ylabel('Query Time (milliseconds)', fontsize=12)
    plt.grid(axis='y', alpha=0.3)

    # Add value labels
    for bar, time in zip(bars, times):
        plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 5,
                f'{time:.1f}ms', ha='center', va='bottom', fontsize=11)

    # Add improvement annotation
    plt.annotate(f'{improvement:.1f}% faster',
                xy=(1, with_index), xytext=(0.5, max(times) * 0.8),
                ha='center', fontsize=12, fontweight='bold',
                bbox=dict(boxstyle='round,pad=0.3', facecolor='yellow', alpha=0.5),
                arrowprops=dict(arrowstyle='->', color='orange'))

    output_path = os.path.join(OUTPUT_DIR, 'query_performance.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Saved: {output_path}")

def plot_compound_index_performance(perf_data):
    """Plot compound index performance"""
    if not perf_data or 'compound_query' not in perf_data:
        print("No compound index performance data available")
        return

    compound = perf_data['compound_query']
    without_compound = compound.get('without_index_ms', 0)
    with_compound = compound.get('with_index_ms', 0)

    if without_compound == 0 or with_compound == 0:
        print("Incomplete compound index data")
        return

    improvement = ((without_compound - with_compound) / without_compound * 100) if without_compound > 0 else 0

    labels = ['Without Compound Index', 'With Compound Index']
    times = [without_compound, with_compound]

    plt.figure(figsize=(12, 6))

    bars = plt.bar(labels, times, color=['orange', 'blue'], alpha=0.7)
    plt.title('Compound Index Performance: (pickup_location_id, passenger_count)', fontsize=16, fontweight='bold')
    plt.ylabel('Query Time (milliseconds)', fontsize=12)
    plt.grid(axis='y', alpha=0.3)

    for bar, time in zip(bars, times):
        plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 5,
                f'{time:.1f}ms', ha='center', va='bottom', fontsize=11)

    if improvement > 0:
        plt.annotate(f'{improvement:.1f}% faster',
                    xy=(1, with_compound), xytext=(0.5, max(times) * 0.8),
                    ha='center', fontsize=12, fontweight='bold',
                    bbox=dict(boxstyle='round,pad=0.3', facecolor='lightblue', alpha=0.5),
                    arrowprops=dict(arrowstyle='->', color='blue'))

    output_path = os.path.join(OUTPUT_DIR, 'compound_index_performance.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Saved: {output_path}")

def display_summary_stats(summary):
    """Display key summary statistics"""
    if not summary:
        return

    print("\n" + "="*60)
    print("TAXI TRIP SUMMARY STATISTICS")
    print("="*60)

    print(f"Total Trips: {summary.get('total_trips', 'N/A'):,}")
    print(f"Average Trip Distance: {summary.get('avg_trip_distance', 'N/A'):.2f} miles")
    print(f"Average Fare Amount: ${summary.get('avg_fare_amount', 'N/A'):.2f}")
    print(f"Average Tip Percent: {summary.get('avg_tip_percent', 'N/A'):.1f}%")
    print(f"Average Speed: {summary.get('avg_speed_mph', 'N/A'):.1f} mph")
    print(f"Total Revenue: ${summary.get('total_revenue', 'N/A'):,.2f}")

    if 'max_trip_distance' in summary:
        print(f"Longest Trip: {summary['max_trip_distance']:.1f} miles")
    if 'min_trip_distance' in summary:
        print(f"Shortest Trip: {summary['min_trip_distance']:.1f} miles")

def main():
    """Main visualization function"""
    print("Big Data Taxi Pipeline - Visualization Script")
    print("="*50)

    try:
        client = connect_mongodb()

        # Load data
        summary = load_aggregated_data(client)
        perf_data = load_performance_data(client)

        if not summary and not perf_data:
            print("No data available. Run the pipeline first with: .\\run.ps1")
            return

        # Display summary stats
        display_summary_stats(summary)

        # Create visualizations
        print("\nGenerating visualizations...")

        if summary:
            plot_trips_by_hour(summary)
            plot_top_locations(summary)
            plot_payment_types(summary)

        if perf_data:
            plot_performance_benchmark(perf_data)
            plot_compound_index_performance(perf_data)

        print("\n" + "="*60)
        print("VISUALIZATION COMPLETE")
        print("="*60)
        print("Generated files:")
        if summary:
            print("  - trips_by_hour.png")
            print("  - top_pickup_locations.png")
            print("  - payment_types.png")
        if perf_data:
            print("  - query_performance.png")
            print("  - compound_index_performance.png")
        print("\nOpen the PNG files to view the charts.")

    except Exception as e:
        print(f"Error: {e}")
        print("Make sure MongoDB is running and the pipeline has been executed.")

if __name__ == "__main__":
=======
"""
Visualization Script for Big Data Taxi Pipeline Results
Creates charts and graphs from MongoDB aggregated data and performance benchmarks
"""

import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError, ConnectionFailure
import os
import json
from datetime import datetime

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), '..', 'visualized_results')
os.makedirs(OUTPUT_DIR, exist_ok=True)
print(f"Saving visualizations to: {os.path.abspath(OUTPUT_DIR)}")

# Set up plotting style
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

def connect_mongodb():
    """Connect to MongoDB with fallback logic"""
    mongodb_uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017/?directConnection=true")
    docker_uri = os.getenv(
        "DOCKER_MONGODB_URI",
        "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0",
    )

    try:
        print(f"Connecting to MongoDB at {mongodb_uri}")
        client = MongoClient(mongodb_uri, serverSelectionTimeoutMS=5000, connectTimeoutMS=5000)
        client.admin.command('ping')
        print("Connected to MongoDB")
        return client
    except (ServerSelectionTimeoutError, ConnectionFailure):
        print("Local MongoDB not available, trying Docker replica set...")
        try:
            client = MongoClient(docker_uri, serverSelectionTimeoutMS=10000, connectTimeoutMS=10000)
            client.admin.command('ping')
            print("Connected to Docker MongoDB replica set")
            return client
        except (ServerSelectionTimeoutError, ConnectionFailure) as e:
            print(f"Failed to connect to MongoDB: {e}")
            print("Make sure MongoDB is running:")
            print("  - For Docker: Run '.\\run.ps1'")
            print("  - For local: Install MongoDB and start mongod service")
            raise

def load_aggregated_data(client):
    """Load aggregated summary data"""
    try:
        summary = client['aggregated']['summary'].find_one()
        if summary:
            print("Loaded aggregated summary data")
            return summary
        else:
            print("No aggregated summary data found. Run the pipeline first.")
            return None
    except Exception as e:
        print(f"Error loading aggregated data: {e}")
        return None

def load_performance_data(client):
    """Load performance benchmark data"""
    try:
        perf = client['raw']['performance_results'].find_one()
        if perf:
            print("Loaded performance benchmark data")
            return perf
        else:
            print("No performance data found. Run the pipeline first.")
            return None
    except Exception as e:
        print(f"Error loading performance data: {e}")
        return None

def plot_trips_by_hour(summary):
    """Plot trips distribution by pickup hour"""
    if 'trips_by_hour' not in summary or not summary['trips_by_hour']:
        print("No trips by hour data available")
        return

    hours_data = summary['trips_by_hour']
    df = pd.DataFrame(hours_data)

    plt.figure(figsize=(12, 6))
    plt.bar(df['pickup_hour'], df['count'], color='skyblue', alpha=0.7)
    plt.title('Taxi Trips by Pickup Hour', fontsize=16, fontweight='bold')
    plt.xlabel('Hour of Day (0-23)', fontsize=12)
    plt.ylabel('Number of Trips', fontsize=12)
    plt.xticks(range(0, 24, 2))
    plt.grid(axis='y', alpha=0.3)

    # Highlight peak hours
    peak_hour = df.loc[df['count'].idxmax()]
    plt.axvline(x=peak_hour['pickup_hour'], color='red', linestyle='--', alpha=0.7,
                label=f'Peak: {peak_hour["pickup_hour"]}:00 ({peak_hour["count"]:,} trips)')
    plt.legend()

    plt.tight_layout()
    output_path = os.path.join(OUTPUT_DIR, 'trips_by_hour.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()  # Use close() instead of show() to avoid GUI popups
    print(f"Saved: {output_path}")

def plot_top_locations(summary):
    """Plot top pickup locations"""
    if 'top_pickup_locations' not in summary or not summary['top_pickup_locations']:
        print("No top pickup locations data available")
        return

    locations_data = summary['top_pickup_locations']
    df = pd.DataFrame(locations_data)

    plt.figure(figsize=(10, 6))
    bars = plt.bar(range(len(df)), df['count'], color='lightgreen', alpha=0.7)
    plt.title('Top 5 Pickup Locations', fontsize=16, fontweight='bold')
    plt.xlabel('Location ID', fontsize=12)
    plt.ylabel('Number of Trips', fontsize=12)
    plt.xticks(range(len(df)), df['pickup_location_id'])
    plt.grid(axis='y', alpha=0.3)

    # Add value labels on bars
    for bar, count in zip(bars, df['count']):
        plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 50,
                f'{count:,}', ha='center', va='bottom', fontsize=10)

    output_path = os.path.join(OUTPUT_DIR, 'top_pickup_locations.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Saved: {output_path}")

def plot_payment_types(summary):
    """Plot payment type distribution"""
    if 'top_payment_types' not in summary or not summary['top_payment_types']:
        print("No payment types data available")
        return

    payment_data = summary['top_payment_types']
    df = pd.DataFrame(payment_data)

    plt.figure(figsize=(8, 6))
    plt.pie(df['count'], labels=df['payment_type'], autopct='%1.1f%%',
            startangle=90, colors=sns.color_palette("pastel"))
    plt.title('Payment Type Distribution', fontsize=16, fontweight='bold')
    plt.axis('equal')

    output_path = os.path.join(OUTPUT_DIR, 'payment_types.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Saved: {output_path}")

def plot_performance_benchmark(perf_data):
    """Plot query performance improvement"""
    if not perf_data:
        print("No performance data available")
        return

    # Extract timing data
    without_index = perf_data.get('metrics', {}).get('without_index_ms', 0)
    with_index = perf_data.get('metrics', {}).get('with_index_ms', 0)
    improvement = perf_data.get('metrics', {}).get('improvement_percent', 0)

    if without_index == 0 or with_index == 0:
        print("Incomplete performance data")
        return

    labels = ['Without Index', 'With Index']
    times = [without_index, with_index]

    plt.figure(figsize=(10, 6))

    # Bar chart
    bars = plt.bar(labels, times, color=['red', 'green'], alpha=0.7)
    plt.title('Query Performance: passenger_count Filter', fontsize=16, fontweight='bold')
    plt.ylabel('Query Time (milliseconds)', fontsize=12)
    plt.grid(axis='y', alpha=0.3)

    # Add value labels
    for bar, time in zip(bars, times):
        plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 5,
                f'{time:.1f}ms', ha='center', va='bottom', fontsize=11)

    # Add improvement annotation
    plt.annotate(f'{improvement:.1f}% faster',
                xy=(1, with_index), xytext=(0.5, max(times) * 0.8),
                ha='center', fontsize=12, fontweight='bold',
                bbox=dict(boxstyle='round,pad=0.3', facecolor='yellow', alpha=0.5),
                arrowprops=dict(arrowstyle='->', color='orange'))

    output_path = os.path.join(OUTPUT_DIR, 'query_performance.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Saved: {output_path}")

def plot_compound_index_performance(perf_data):
    """Plot compound index performance"""
    if not perf_data or 'compound_query' not in perf_data:
        print("No compound index performance data available")
        return

    compound = perf_data['compound_query']
    without_compound = compound.get('without_index_ms', 0)
    with_compound = compound.get('with_index_ms', 0)

    if without_compound == 0 or with_compound == 0:
        print("Incomplete compound index data")
        return

    improvement = ((without_compound - with_compound) / without_compound * 100) if without_compound > 0 else 0

    labels = ['Without Compound Index', 'With Compound Index']
    times = [without_compound, with_compound]

    plt.figure(figsize=(12, 6))

    bars = plt.bar(labels, times, color=['orange', 'blue'], alpha=0.7)
    plt.title('Compound Index Performance: (pickup_location_id, passenger_count)', fontsize=16, fontweight='bold')
    plt.ylabel('Query Time (milliseconds)', fontsize=12)
    plt.grid(axis='y', alpha=0.3)

    for bar, time in zip(bars, times):
        plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 5,
                f'{time:.1f}ms', ha='center', va='bottom', fontsize=11)

    if improvement > 0:
        plt.annotate(f'{improvement:.1f}% faster',
                    xy=(1, with_compound), xytext=(0.5, max(times) * 0.8),
                    ha='center', fontsize=12, fontweight='bold',
                    bbox=dict(boxstyle='round,pad=0.3', facecolor='lightblue', alpha=0.5),
                    arrowprops=dict(arrowstyle='->', color='blue'))

    output_path = os.path.join(OUTPUT_DIR, 'compound_index_performance.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Saved: {output_path}")

def display_summary_stats(summary):
    """Display key summary statistics"""
    if not summary:
        return

    print("\n" + "="*60)
    print("TAXI TRIP SUMMARY STATISTICS")
    print("="*60)

    print(f"Total Trips: {summary.get('total_trips', 'N/A'):,}")
    print(f"Average Trip Distance: {summary.get('avg_trip_distance', 'N/A'):.2f} miles")
    print(f"Average Fare Amount: ${summary.get('avg_fare_amount', 'N/A'):.2f}")
    print(f"Average Tip Percent: {summary.get('avg_tip_percent', 'N/A'):.1f}%")
    print(f"Average Speed: {summary.get('avg_speed_mph', 'N/A'):.1f} mph")
    print(f"Total Revenue: ${summary.get('total_revenue', 'N/A'):,.2f}")

    if 'max_trip_distance' in summary:
        print(f"Longest Trip: {summary['max_trip_distance']:.1f} miles")
    if 'min_trip_distance' in summary:
        print(f"Shortest Trip: {summary['min_trip_distance']:.1f} miles")

def main():
    """Main visualization function"""
    print("Big Data Taxi Pipeline - Visualization Script")
    print("="*50)

    try:
        client = connect_mongodb()

        # Load data
        summary = load_aggregated_data(client)
        perf_data = load_performance_data(client)

        if not summary and not perf_data:
            print("No data available. Run the pipeline first with: .\\run.ps1")
            return

        # Display summary stats
        display_summary_stats(summary)

        # Create visualizations
        print("\nGenerating visualizations...")

        if summary:
            plot_trips_by_hour(summary)
            plot_top_locations(summary)
            plot_payment_types(summary)

        if perf_data:
            plot_performance_benchmark(perf_data)
            plot_compound_index_performance(perf_data)

        print("\n" + "="*60)
        print("VISUALIZATION COMPLETE")
        print("="*60)
        print("Generated files:")
        if summary:
            print("  - trips_by_hour.png")
            print("  - top_pickup_locations.png")
            print("  - payment_types.png")
        if perf_data:
            print("  - query_performance.png")
            print("  - compound_index_performance.png")
        print("\nOpen the PNG files to view the charts.")

    except Exception as e:
        print(f"Error: {e}")
        print("Make sure MongoDB is running and the pipeline has been executed.")

if __name__ == "__main__":
>>>>>>> 31a89514fe5c607ba63bb0574cb71393aba7e6a1
    main()