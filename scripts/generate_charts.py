#!/usr/bin/env python3
"""
D-LOCKSS Metrics Chart Generator
Generates scientific paper-quality charts from CSV metrics data.

Usage:
    python3 generate_charts.py <metrics_csv_file> [output_dir]
    
Example:
    python3 generate_charts.py testnet/testnet_data/node_1/metrics.csv charts/
"""

import sys
import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pathlib import Path
import argparse
from datetime import datetime

# Set style for scientific papers
plt.style.use('seaborn-v0_8-paper')
plt.rcParams.update({
    'font.size': 10,
    'font.family': 'serif',
    'font.serif': ['Times New Roman', 'Times', 'DejaVu Serif'],
    'axes.labelsize': 11,
    'axes.titlesize': 12,
    'xtick.labelsize': 9,
    'ytick.labelsize': 9,
    'legend.fontsize': 9,
    'figure.titlesize': 13,
    'figure.dpi': 300,
    'savefig.dpi': 300,
    'savefig.bbox': 'tight',
    'savefig.pad_inches': 0.1,
    'lines.linewidth': 0.8,
    'axes.linewidth': 0.8,
    'grid.linewidth': 0.5,
    'grid.alpha': 0.3,
})

def load_metrics(csv_path):
    """Load metrics from CSV file."""
    df = pd.read_csv(csv_path)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values('timestamp')
    return df

def plot_storage_metrics(df, output_dir):
    """Plot storage-related metrics."""
    fig, axes = plt.subplots(2, 1, figsize=(8, 6), sharex=True)
    
    # Pinned and known files
    axes[0].plot(df['timestamp'], df['pinned_files'], label='Pinned Files', linewidth=0.8, color='#2E86AB')
    axes[0].plot(df['timestamp'], df['known_files'], label='Known Files', linewidth=0.8, color='#A23B72', linestyle='--')
    axes[0].set_ylabel('File Count', fontsize=11)
    axes[0].set_title('Storage Metrics', fontsize=12, fontweight='bold')
    axes[0].legend(loc='best', frameon=True, fancybox=True, shadow=True)
    axes[0].grid(True, alpha=0.3, linestyle='--')
    
    # Replication status
    axes[1].plot(df['timestamp'], df['low_replication_files'], label='Low Replication', linewidth=0.8, color='#F18F01')
    axes[1].plot(df['timestamp'], df['high_replication_files'], label='High Replication', linewidth=0.8, color='#C73E1D')
    axes[1].set_xlabel('Time', fontsize=11)
    axes[1].set_ylabel('File Count', fontsize=11)
    axes[1].set_title('Replication Status', fontsize=12, fontweight='bold')
    axes[1].legend(loc='best', frameon=True, fancybox=True, shadow=True)
    axes[1].grid(True, alpha=0.3, linestyle='--')
    
    # Format x-axis
    axes[1].xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    axes[1].xaxis.set_major_locator(mdates.MinuteLocator(interval=max(1, len(df) // 10)))
    plt.setp(axes[1].xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'storage_metrics.pdf'), format='pdf')
    plt.savefig(os.path.join(output_dir, 'storage_metrics.png'), format='png')
    plt.close()
    print(f"✓ Generated storage_metrics.pdf and .png")

def plot_network_metrics(df, output_dir):
    """Plot network-related metrics."""
    fig, axes = plt.subplots(3, 1, figsize=(8, 8), sharex=True)
    
    # Message rates
    axes[0].plot(df['timestamp'], df['messages_received'], label='Messages Received', linewidth=0.8, color='#06A77D')
    axes[0].plot(df['timestamp'], df['messages_dropped'], label='Messages Dropped', linewidth=0.8, color='#D00000', linestyle='--')
    axes[0].set_ylabel('Messages', fontsize=11)
    axes[0].set_title('Message Traffic (per reporting period)', fontsize=12, fontweight='bold')
    axes[0].legend(loc='best', frameon=True, fancybox=True, shadow=True)
    axes[0].grid(True, alpha=0.3, linestyle='--')
    
    # Active peers (handle missing column gracefully)
    if 'active_peers' in df.columns:
        axes[1].plot(df['timestamp'], df['active_peers'], label='Active Peers', linewidth=0.8, color='#6A4C93')
    if 'rate_limited_peers' in df.columns:
        axes[1].plot(df['timestamp'], df['rate_limited_peers'], label='Rate Limited Peers', linewidth=0.8, color='#FF6B35', linestyle='--')
    axes[1].set_ylabel('Peer Count', fontsize=11)
    axes[1].set_title('Network Connectivity', fontsize=12, fontweight='bold')
    axes[1].legend(loc='best', frameon=True, fancybox=True, shadow=True)
    axes[1].grid(True, alpha=0.3, linestyle='--')
    
    # DHT queries
    axes[2].plot(df['timestamp'], df['dht_queries'], label='DHT Queries', linewidth=0.8, color='#118AB2')
    axes[2].plot(df['timestamp'], df['dht_query_timeouts'], label='DHT Timeouts', linewidth=0.8, color='#EF476F', linestyle='--')
    axes[2].set_xlabel('Time', fontsize=11)
    axes[2].set_ylabel('Query Count', fontsize=11)
    axes[2].set_title('DHT Query Activity', fontsize=12, fontweight='bold')
    axes[2].legend(loc='best', frameon=True, fancybox=True, shadow=True)
    axes[2].grid(True, alpha=0.3, linestyle='--')
    
    # Format x-axis
    axes[2].xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    axes[2].xaxis.set_major_locator(mdates.MinuteLocator(interval=max(1, len(df) // 10)))
    plt.setp(axes[2].xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'network_metrics.pdf'), format='pdf')
    plt.savefig(os.path.join(output_dir, 'network_metrics.png'), format='png')
    plt.close()
    print(f"✓ Generated network_metrics.pdf and .png")

def plot_replication_metrics(df, output_dir):
    """Plot replication-related metrics."""
    fig, axes = plt.subplots(2, 1, figsize=(8, 6), sharex=True)
    
    # Replication checks
    axes[0].plot(df['timestamp'], df['replication_checks'], label='Checks', linewidth=0.8, color='#2E86AB')
    axes[0].plot(df['timestamp'], df['replication_success'], label='Success', linewidth=0.8, color='#06A77D', linestyle='--')
    axes[0].plot(df['timestamp'], df['replication_failures'], label='Failures', linewidth=0.8, color='#D00000', linestyle=':')
    axes[0].set_ylabel('Count', fontsize=11)
    axes[0].set_title('Replication Check Activity (per reporting period)', fontsize=12, fontweight='bold')
    axes[0].legend(loc='best', frameon=True, fancybox=True, shadow=True)
    axes[0].grid(True, alpha=0.3, linestyle='--')
    
    # Cumulative replication metrics
    axes[1].plot(df['timestamp'], df['cumulative_replication_checks'], label='Total Checks', linewidth=0.8, color='#2E86AB')
    axes[1].plot(df['timestamp'], df['cumulative_replication_success'], label='Total Success', linewidth=0.8, color='#06A77D', linestyle='--')
    axes[1].plot(df['timestamp'], df['cumulative_replication_failures'], label='Total Failures', linewidth=0.8, color='#D00000', linestyle=':')
    axes[1].set_xlabel('Time', fontsize=11)
    axes[1].set_ylabel('Cumulative Count', fontsize=11)
    axes[1].set_title('Cumulative Replication Metrics', fontsize=12, fontweight='bold')
    axes[1].legend(loc='best', frameon=True, fancybox=True, shadow=True)
    axes[1].grid(True, alpha=0.3, linestyle='--')
    
    # Format x-axis
    axes[1].xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    axes[1].xaxis.set_major_locator(mdates.MinuteLocator(interval=max(1, len(df) // 10)))
    plt.setp(axes[1].xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'replication_metrics.pdf'), format='pdf')
    plt.savefig(os.path.join(output_dir, 'replication_metrics.png'), format='png')
    plt.close()
    print(f"✓ Generated replication_metrics.pdf and .png")

def plot_performance_metrics(df, output_dir):
    """Plot performance-related metrics."""
    fig, axes = plt.subplots(2, 1, figsize=(8, 6), sharex=True)
    
    # Worker pool utilization
    max_workers = df['worker_pool_active'].max() + df['worker_pool_active'].max() * 0.1  # Add 10% padding
    axes[0].plot(df['timestamp'], df['worker_pool_active'], label='Active Workers', linewidth=0.8, color='#6A4C93')
    axes[0].axhline(y=max_workers * 0.8, color='r', linestyle='--', alpha=0.5, label='80% Threshold')
    axes[0].set_ylabel('Worker Count', fontsize=11)
    axes[0].set_title('Worker Pool Utilization', fontsize=12, fontweight='bold')
    axes[0].legend(loc='best', frameon=True, fancybox=True, shadow=True)
    axes[0].grid(True, alpha=0.3, linestyle='--')
    
    # Files in backoff
    axes[1].plot(df['timestamp'], df['files_in_backoff'], label='Files in Backoff', linewidth=0.8, color='#F18F01')
    axes[1].set_xlabel('Time', fontsize=11)
    axes[1].set_ylabel('File Count', fontsize=11)
    axes[1].set_title('Failed Operations (Backoff)', fontsize=12, fontweight='bold')
    axes[1].legend(loc='best', frameon=True, fancybox=True, shadow=True)
    axes[1].grid(True, alpha=0.3, linestyle='--')
    
    # Format x-axis
    axes[1].xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    axes[1].xaxis.set_major_locator(mdates.MinuteLocator(interval=max(1, len(df) // 10)))
    plt.setp(axes[1].xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'performance_metrics.pdf'), format='pdf')
    plt.savefig(os.path.join(output_dir, 'performance_metrics.png'), format='png')
    plt.close()
    print(f"✓ Generated performance_metrics.pdf and .png")

def plot_cumulative_metrics(df, output_dir):
    """Plot cumulative metrics over time."""
    fig, axes = plt.subplots(2, 1, figsize=(8, 6), sharex=True)
    
    # Cumulative messages
    axes[0].plot(df['timestamp'], df['cumulative_messages_received'], label='Total Received', linewidth=0.8, color='#06A77D')
    axes[0].plot(df['timestamp'], df['cumulative_messages_dropped'], label='Total Dropped', linewidth=0.8, color='#D00000', linestyle='--')
    axes[0].set_ylabel('Message Count', fontsize=11)
    axes[0].set_title('Cumulative Message Traffic', fontsize=12, fontweight='bold')
    axes[0].legend(loc='best', frameon=True, fancybox=True, shadow=True)
    axes[0].grid(True, alpha=0.3, linestyle='--')
    
    # Cumulative DHT queries
    axes[1].plot(df['timestamp'], df['cumulative_dht_queries'], label='Total Queries', linewidth=0.8, color='#118AB2')
    axes[1].plot(df['timestamp'], df['cumulative_dht_query_timeouts'], label='Total Timeouts', linewidth=0.8, color='#EF476F', linestyle='--')
    axes[1].set_xlabel('Time', fontsize=11)
    axes[1].set_ylabel('Query Count', fontsize=11)
    axes[1].set_title('Cumulative DHT Activity', fontsize=12, fontweight='bold')
    axes[1].legend(loc='best', frameon=True, fancybox=True, shadow=True)
    axes[1].grid(True, alpha=0.3, linestyle='--')
    
    # Format x-axis
    axes[1].xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    axes[1].xaxis.set_major_locator(mdates.MinuteLocator(interval=max(1, len(df) // 10)))
    plt.setp(axes[1].xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'cumulative_metrics.pdf'), format='pdf')
    plt.savefig(os.path.join(output_dir, 'cumulative_metrics.png'), format='png')
    plt.close()
    print(f"✓ Generated cumulative_metrics.pdf and .png")

def generate_summary_stats(df, output_dir):
    """Generate summary statistics text file."""
    stats = []
    stats.append("=" * 60)
    stats.append("D-LOCKSS Testnet Run Summary Statistics")
    stats.append("=" * 60)
    stats.append(f"\nTest Duration: {df['uptime_seconds'].iloc[-1]:.2f} seconds ({df['uptime_seconds'].iloc[-1]/60:.2f} minutes)")
    stats.append(f"Start Time: {df['timestamp'].iloc[0]}")
    stats.append(f"End Time: {df['timestamp'].iloc[-1]}")
    stats.append(f"\n--- Storage Metrics ---")
    stats.append(f"Max Pinned Files: {df['pinned_files'].max()}")
    stats.append(f"Max Known Files: {df['known_files'].max()}")
    stats.append(f"Final Pinned Files: {df['pinned_files'].iloc[-1]}")
    stats.append(f"Final Known Files: {df['known_files'].iloc[-1]}")
    stats.append(f"\n--- Network Metrics ---")
    stats.append(f"Total Messages Received: {df['cumulative_messages_received'].iloc[-1]}")
    stats.append(f"Total Messages Dropped: {df['cumulative_messages_dropped'].iloc[-1]}")
    if 'active_peers' in df.columns:
        stats.append(f"Max Active Peers: {df['active_peers'].max()}")
        stats.append(f"Final Active Peers: {df['active_peers'].iloc[-1]}")
    elif 'rate_limited_peers' in df.columns:
        stats.append(f"Max Active Peers: {df['rate_limited_peers'].max()}")
        stats.append(f"Final Active Peers: {df['rate_limited_peers'].iloc[-1]}")
    stats.append(f"\n--- Replication Metrics ---")
    stats.append(f"Total Replication Checks: {df['cumulative_replication_checks'].iloc[-1]}")
    stats.append(f"Total Success: {df['cumulative_replication_success'].iloc[-1]}")
    stats.append(f"Total Failures: {df['cumulative_replication_failures'].iloc[-1]}")
    if df['cumulative_replication_checks'].iloc[-1] > 0:
        success_rate = (df['cumulative_replication_success'].iloc[-1] / df['cumulative_replication_checks'].iloc[-1]) * 100
        stats.append(f"Success Rate: {success_rate:.2f}%")
    stats.append(f"\n--- DHT Metrics ---")
    stats.append(f"Total DHT Queries: {df['cumulative_dht_queries'].iloc[-1]}")
    stats.append(f"Total DHT Timeouts: {df['cumulative_dht_query_timeouts'].iloc[-1]}")
    if df['cumulative_dht_queries'].iloc[-1] > 0:
        timeout_rate = (df['cumulative_dht_query_timeouts'].iloc[-1] / df['cumulative_dht_queries'].iloc[-1]) * 100
        stats.append(f"Timeout Rate: {timeout_rate:.2f}%")
    stats.append(f"\n--- Performance Metrics ---")
    stats.append(f"Max Worker Pool Active: {df['worker_pool_active'].max()}")
    stats.append(f"Max Files in Backoff: {df['files_in_backoff'].max()}")
    stats.append(f"Total Shard Splits: {df['cumulative_shard_splits'].iloc[-1]}")
    stats.append(f"Final Shard: {df['current_shard'].iloc[-1]}")
    stats.append("\n" + "=" * 60)
    
    with open(os.path.join(output_dir, 'summary_stats.txt'), 'w') as f:
        f.write('\n'.join(stats))
    
    print(f"✓ Generated summary_stats.txt")

def find_metrics_csvs(search_dir='testnet/testnet_data'):
    """Find all metrics.csv files in testnet node directories."""
    csv_files = []
    search_path = Path(search_dir)
    
    # If relative path doesn't exist, try relative to script directory
    if not search_path.exists():
        script_dir = Path(__file__).parent.parent
        alt_path = script_dir / search_dir
        if alt_path.exists():
            search_path = alt_path
        else:
            # Try relative to current working directory
            cwd_path = Path.cwd() / search_dir
            if cwd_path.exists():
                search_path = cwd_path
            else:
                print(f"Warning: Search directory not found: {search_dir}")
                print(f"  Tried: {Path(search_dir).absolute()}")
                print(f"  Tried: {script_dir / search_dir}")
                print(f"  Tried: {Path.cwd() / search_dir}")
                return csv_files
    
    # Look for metrics.csv in node_X directories
    for node_dir in sorted(search_path.glob('node_*/metrics.csv')):
        csv_files.append(str(node_dir))
    
    # Also check direct subdirectories
    for csv_file in sorted(search_path.glob('*/metrics.csv')):
        if str(csv_file) not in csv_files:
            csv_files.append(str(csv_file))
    
    return csv_files

def create_timestamped_output_dir(base_dir='charts'):
    """Create a timestamped output directory."""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_dir = os.path.join(base_dir, f'charts_{timestamp}')
    os.makedirs(output_dir, exist_ok=True)
    return output_dir

def load_all_metrics(csv_files):
    """Load and combine metrics from all CSV files."""
    all_data = []
    node_names = []
    
    for csv_file in csv_files:
        try:
            df = load_metrics(csv_file)
            node_name = Path(csv_file).parent.name
            df['node'] = node_name
            # Round timestamps to 10-second intervals for proper alignment
            # This ensures nodes reporting at slightly different times are grouped correctly
            df['timestamp'] = df['timestamp'].dt.floor('10S')
            all_data.append(df)
            node_names.append(node_name)
            print(f"  Loaded {len(df)} data points from {node_name}")
        except Exception as e:
            print(f"  Warning: Failed to load {csv_file}: {e}")
            continue
    
    if not all_data:
        return None, []
    
    # Combine all dataframes
    combined_df = pd.concat(all_data, ignore_index=True)
    return combined_df, node_names

def plot_overview_storage_metrics(df, output_dir):
    """Plot aggregated storage metrics across all nodes."""
    fig, axes = plt.subplots(2, 1, figsize=(10, 7), sharex=True)
    
    # Aggregate by timestamp (sum across all nodes)
    aggregated = df.groupby('timestamp').agg({
        'pinned_files': 'sum',
        'known_files': 'sum',
        'low_replication_files': 'sum',
        'high_replication_files': 'sum'
    }).reset_index()
    
    # Pinned and known files (network-wide totals)
    axes[0].plot(aggregated['timestamp'], aggregated['pinned_files'], 
                 label='Total Pinned Files (Network)', linewidth=1.0, color='#2E86AB')
    axes[0].plot(aggregated['timestamp'], aggregated['known_files'], 
                 label='Total Known Files (Network)', linewidth=1.0, color='#A23B72', linestyle='--')
    axes[0].set_ylabel('File Count', fontsize=11)
    axes[0].set_title('Network-Wide Storage Metrics', fontsize=12, fontweight='bold')
    axes[0].legend(loc='best', frameon=True, fancybox=True, shadow=True)
    axes[0].grid(True, alpha=0.3, linestyle='--')
    
    # Replication status (network-wide)
    axes[1].plot(aggregated['timestamp'], aggregated['low_replication_files'], 
                 label='Low Replication (Total)', linewidth=1.0, color='#F18F01')
    axes[1].plot(aggregated['timestamp'], aggregated['high_replication_files'], 
                 label='High Replication (Total)', linewidth=1.0, color='#C73E1D')
    axes[1].set_xlabel('Time', fontsize=11)
    axes[1].set_ylabel('File Count', fontsize=11)
    axes[1].set_title('Network-Wide Replication Status', fontsize=12, fontweight='bold')
    axes[1].legend(loc='best', frameon=True, fancybox=True, shadow=True)
    axes[1].grid(True, alpha=0.3, linestyle='--')
    
    # Format x-axis
    axes[1].xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    axes[1].xaxis.set_major_locator(mdates.MinuteLocator(interval=max(1, len(aggregated) // 10)))
    plt.setp(axes[1].xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'overview_storage_metrics.png'), format='png', dpi=300)
    plt.close()
    print(f"✓ Generated overview_storage_metrics.png")

def plot_overview_network_metrics(df, output_dir):
    """Plot aggregated network metrics across all nodes."""
    fig, axes = plt.subplots(3, 1, figsize=(10, 9), sharex=True)
    
    # Aggregate by timestamp
    agg_dict = {
        'messages_received': 'sum',
        'messages_dropped': 'sum',
        'dht_queries': 'sum',
        'dht_query_timeouts': 'sum'
    }
    
    # Handle peer columns gracefully
    if 'active_peers' in df.columns:
        agg_dict['active_peers'] = 'max'
    if 'rate_limited_peers' in df.columns:
        agg_dict['rate_limited_peers'] = 'max'
    
    aggregated = df.groupby('timestamp').agg(agg_dict).reset_index()
    
    # Message rates (network-wide totals)
    axes[0].plot(aggregated['timestamp'], aggregated['messages_received'], 
                 label='Total Messages Received', linewidth=1.0, color='#06A77D')
    axes[0].plot(aggregated['timestamp'], aggregated['messages_dropped'], 
                 label='Total Messages Dropped', linewidth=1.0, color='#D00000', linestyle='--')
    axes[0].set_ylabel('Messages', fontsize=11)
    axes[0].set_title('Network-Wide Message Traffic (per reporting period)', fontsize=12, fontweight='bold')
    axes[0].legend(loc='best', frameon=True, fancybox=True, shadow=True)
    axes[0].grid(True, alpha=0.3, linestyle='--')
    
    # Active peers (max across nodes)
    if 'active_peers' in df.columns:
        axes[1].plot(aggregated['timestamp'], aggregated['active_peers'], 
                     label='Max Active Peers (Any Node)', linewidth=1.0, color='#6A4C93')
    if 'rate_limited_peers' in df.columns:
        axes[1].plot(aggregated['timestamp'], aggregated['rate_limited_peers'], 
                     label='Max Rate Limited Peers', linewidth=1.0, color='#FF6B35', linestyle='--')
    axes[1].set_ylabel('Peer Count', fontsize=11)
    axes[1].set_title('Network Connectivity (Peak Values)', fontsize=12, fontweight='bold')
    axes[1].legend(loc='best', frameon=True, fancybox=True, shadow=True)
    axes[1].grid(True, alpha=0.3, linestyle='--')
    
    # DHT queries (network-wide totals)
    axes[2].plot(aggregated['timestamp'], aggregated['dht_queries'], 
                 label='Total DHT Queries', linewidth=1.0, color='#118AB2')
    axes[2].plot(aggregated['timestamp'], aggregated['dht_query_timeouts'], 
                 label='Total DHT Timeouts', linewidth=1.0, color='#EF476F', linestyle='--')
    axes[2].set_xlabel('Time', fontsize=11)
    axes[2].set_ylabel('Query Count', fontsize=11)
    axes[2].set_title('Network-Wide DHT Activity', fontsize=12, fontweight='bold')
    axes[2].legend(loc='best', frameon=True, fancybox=True, shadow=True)
    axes[2].grid(True, alpha=0.3, linestyle='--')
    
    # Format x-axis
    axes[2].xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    axes[2].xaxis.set_major_locator(mdates.MinuteLocator(interval=max(1, len(aggregated) // 10)))
    plt.setp(axes[2].xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'overview_network_metrics.png'), format='png', dpi=300)
    plt.close()
    print(f"✓ Generated overview_network_metrics.png")

def plot_overview_replication_metrics(df, output_dir):
    """Plot aggregated replication metrics across all nodes."""
    fig, axes = plt.subplots(2, 1, figsize=(10, 7), sharex=True)
    
    # Aggregate by timestamp
    aggregated = df.groupby('timestamp').agg({
        'replication_checks': 'sum',
        'replication_success': 'sum',
        'replication_failures': 'sum',
        'cumulative_replication_checks': 'sum',
        'cumulative_replication_success': 'sum',
        'cumulative_replication_failures': 'sum'
    }).reset_index()
    
    # Replication checks (network-wide totals)
    axes[0].plot(aggregated['timestamp'], aggregated['replication_checks'], 
                 label='Total Checks', linewidth=1.0, color='#2E86AB')
    axes[0].plot(aggregated['timestamp'], aggregated['replication_success'], 
                 label='Total Success', linewidth=1.0, color='#06A77D', linestyle='--')
    axes[0].plot(aggregated['timestamp'], aggregated['replication_failures'], 
                 label='Total Failures', linewidth=1.0, color='#D00000', linestyle=':')
    axes[0].set_ylabel('Count', fontsize=11)
    axes[0].set_title('Network-Wide Replication Check Activity (per reporting period)', fontsize=12, fontweight='bold')
    axes[0].legend(loc='best', frameon=True, fancybox=True, shadow=True)
    axes[0].grid(True, alpha=0.3, linestyle='--')
    
    # Cumulative replication metrics (network-wide)
    axes[1].plot(aggregated['timestamp'], aggregated['cumulative_replication_checks'], 
                 label='Total Checks (Cumulative)', linewidth=1.0, color='#2E86AB')
    axes[1].plot(aggregated['timestamp'], aggregated['cumulative_replication_success'], 
                 label='Total Success (Cumulative)', linewidth=1.0, color='#06A77D', linestyle='--')
    axes[1].plot(aggregated['timestamp'], aggregated['cumulative_replication_failures'], 
                 label='Total Failures (Cumulative)', linewidth=1.0, color='#D00000', linestyle=':')
    axes[1].set_xlabel('Time', fontsize=11)
    axes[1].set_ylabel('Cumulative Count', fontsize=11)
    axes[1].set_title('Network-Wide Cumulative Replication Metrics', fontsize=12, fontweight='bold')
    axes[1].legend(loc='best', frameon=True, fancybox=True, shadow=True)
    axes[1].grid(True, alpha=0.3, linestyle='--')
    
    # Format x-axis
    axes[1].xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    axes[1].xaxis.set_major_locator(mdates.MinuteLocator(interval=max(1, len(aggregated) // 10)))
    plt.setp(axes[1].xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'overview_replication_metrics.png'), format='png', dpi=300)
    plt.close()
    print(f"✓ Generated overview_replication_metrics.png")

def plot_overview_convergence_metrics(df, output_dir):
    """Plot replication convergence metrics across all nodes."""
    # Check if convergence columns exist
    required_cols = ['files_at_target_replication', 'avg_replication_level', 
                     'files_converged_total', 'files_converged_this_period']
    if not all(col in df.columns for col in required_cols):
        print(f"⚠ Skipping convergence metrics (missing columns)")
        return
    
    fig, axes = plt.subplots(3, 1, figsize=(10, 9), sharex=True)
    
    # Aggregate by timestamp
    aggregated = df.groupby('timestamp').agg({
        'files_at_target_replication': 'sum',
        'low_replication_files': 'sum',
        'high_replication_files': 'sum',
        'avg_replication_level': 'mean',  # Average of averages
        'files_converged_total': 'sum',
        'files_converged_this_period': 'sum'
    }).reset_index()
    
    # Files at target replication (convergence status)
    axes[0].plot(aggregated['timestamp'], aggregated['files_at_target_replication'], 
                 label='Files at Target (5-10 copies)', linewidth=1.0, color='#06A77D')
    axes[0].plot(aggregated['timestamp'], aggregated['low_replication_files'], 
                 label='Low Replication (<5 copies)', linewidth=1.0, color='#F18F01', linestyle='--')
    axes[0].plot(aggregated['timestamp'], aggregated['high_replication_files'], 
                 label='High Replication (>10 copies)', linewidth=1.0, color='#C73E1D', linestyle=':')
    axes[0].set_ylabel('File Count', fontsize=11)
    axes[0].set_title('Network-Wide Replication Status Distribution', fontsize=12, fontweight='bold')
    axes[0].legend(loc='best', frameon=True, fancybox=True, shadow=True)
    axes[0].grid(True, alpha=0.3, linestyle='--')
    
    # Average replication level
    axes[1].plot(aggregated['timestamp'], aggregated['avg_replication_level'], 
                 label='Average Replication Level', linewidth=1.0, color='#2E86AB')
    axes[1].axhline(y=5, color='#06A77D', linestyle='--', linewidth=1, label='Min Target (5)')
    axes[1].axhline(y=10, color='#06A77D', linestyle='--', linewidth=1, label='Max Target (10)')
    axes[1].set_ylabel('Replication Level', fontsize=11)
    axes[1].set_title('Network-Wide Average Replication Level', fontsize=12, fontweight='bold')
    axes[1].legend(loc='best', frameon=True, fancybox=True, shadow=True)
    axes[1].grid(True, alpha=0.3, linestyle='--')
    
    # Convergence rate
    axes[2].plot(aggregated['timestamp'], aggregated['files_converged_total'], 
                 label='Total Files Converged (Cumulative)', linewidth=1.0, color='#6A4C93')
    axes[2].plot(aggregated['timestamp'], aggregated['files_converged_this_period'], 
                 label='Files Converged This Period', linewidth=1.0, color='#A23B72', linestyle='--')
    axes[2].set_xlabel('Time', fontsize=11)
    axes[2].set_ylabel('File Count', fontsize=11)
    axes[2].set_title('Network-Wide Convergence Rate', fontsize=12, fontweight='bold')
    axes[2].legend(loc='best', frameon=True, fancybox=True, shadow=True)
    axes[2].grid(True, alpha=0.3, linestyle='--')
    
    # Format x-axis
    axes[2].xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    axes[2].xaxis.set_major_locator(mdates.MinuteLocator(interval=max(1, len(aggregated) // 10)))
    plt.setp(axes[2].xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'overview_convergence_metrics.png'), format='png', dpi=300)
    plt.close()
    print(f"✓ Generated overview_convergence_metrics.png")

def plot_overview_replication_distribution(df, output_dir):
    """Plot replication level distribution across all nodes."""
    # Check if distribution columns exist
    repl_cols = [f'repl_level_{i}' for i in range(11)]
    if not all(col in df.columns for col in repl_cols):
        print(f"⚠ Skipping replication distribution (missing columns)")
        return
    
    fig, axes = plt.subplots(2, 1, figsize=(10, 7), sharex=True)
    
    # Aggregate by timestamp
    aggregated = df.groupby('timestamp').agg({
        col: 'sum' for col in repl_cols
    }).reset_index()
    
    # Stacked area chart showing distribution over time
    colors = ['#D00000', '#F18F01', '#FFC300', '#FFD60A', '#FFE66D',  # 0-4: Red to Yellow
              '#06A77D', '#06A77D', '#06A77D', '#06A77D', '#06A77D', '#06A77D']  # 5-10+: Green
    labels = [f'Level {i}' if i < 10 else 'Level 10+' for i in range(11)]
    
    axes[0].stackplot(aggregated['timestamp'],
                      [aggregated[f'repl_level_{i}'] for i in range(11)],
                      labels=labels, colors=colors, alpha=0.7)
    axes[0].set_ylabel('File Count', fontsize=11)
    axes[0].set_title('Network-Wide Replication Level Distribution (Stacked)', fontsize=12, fontweight='bold')
    axes[0].legend(loc='upper left', frameon=True, fancybox=True, shadow=True, ncol=3, fontsize=8)
    axes[0].grid(True, alpha=0.3, linestyle='--')
    
    # Line chart showing key levels
    axes[1].plot(aggregated['timestamp'], aggregated['repl_level_0'], 
                 label='Level 0 (No copies)', linewidth=0.8, color='#D00000')
    axes[1].plot(aggregated['timestamp'], aggregated['repl_level_5'], 
                 label='Level 5 (Min target)', linewidth=0.8, color='#06A77D')
    axes[1].plot(aggregated['timestamp'], aggregated['repl_level_10plus'], 
                 label='Level 10+ (Over-replicated)', linewidth=0.8, color='#C73E1D', linestyle='--')
    axes[1].set_xlabel('Time', fontsize=11)
    axes[1].set_ylabel('File Count', fontsize=11)
    axes[1].set_title('Key Replication Levels Over Time', fontsize=12, fontweight='bold')
    axes[1].legend(loc='best', frameon=True, fancybox=True, shadow=True)
    axes[1].grid(True, alpha=0.3, linestyle='--')
    
    # Format x-axis
    axes[1].xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    axes[1].xaxis.set_major_locator(mdates.MinuteLocator(interval=max(1, len(aggregated) // 10)))
    plt.setp(axes[1].xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'overview_replication_distribution.png'), format='png', dpi=300)
    plt.close()
    print(f"✓ Generated overview_replication_distribution.png")

def plot_overview_performance_metrics(df, output_dir):
    """Plot aggregated performance metrics across all nodes."""
    fig, axes = plt.subplots(2, 1, figsize=(10, 7), sharex=True)
    
    # Aggregate by timestamp
    aggregated = df.groupby('timestamp').agg({
        'worker_pool_active': 'sum',
        'files_in_backoff': 'sum'
    }).reset_index()
    
    # Worker pool utilization (network-wide total)
    axes[0].plot(aggregated['timestamp'], aggregated['worker_pool_active'], 
                 label='Total Active Workers (Network)', linewidth=1.0, color='#6A4C93')
    axes[0].set_ylabel('Worker Count', fontsize=11)
    axes[0].set_title('Network-Wide Worker Pool Utilization', fontsize=12, fontweight='bold')
    axes[0].legend(loc='best', frameon=True, fancybox=True, shadow=True)
    axes[0].grid(True, alpha=0.3, linestyle='--')
    
    # Files in backoff (network-wide total)
    axes[1].plot(aggregated['timestamp'], aggregated['files_in_backoff'], 
                 label='Total Files in Backoff (Network)', linewidth=1.0, color='#F18F01')
    axes[1].set_xlabel('Time', fontsize=11)
    axes[1].set_ylabel('File Count', fontsize=11)
    axes[1].set_title('Network-Wide Failed Operations (Backoff)', fontsize=12, fontweight='bold')
    axes[1].legend(loc='best', frameon=True, fancybox=True, shadow=True)
    axes[1].grid(True, alpha=0.3, linestyle='--')
    
    # Format x-axis
    axes[1].xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    axes[1].xaxis.set_major_locator(mdates.MinuteLocator(interval=max(1, len(aggregated) // 10)))
    plt.setp(axes[1].xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'overview_performance_metrics.png'), format='png', dpi=300)
    plt.close()
    print(f"✓ Generated overview_performance_metrics.png")

def plot_overview_cumulative_metrics(df, output_dir):
    """Plot aggregated cumulative metrics across all nodes."""
    fig, axes = plt.subplots(2, 1, figsize=(10, 7), sharex=True)
    
    # Aggregate by timestamp
    aggregated = df.groupby('timestamp').agg({
        'cumulative_messages_received': 'sum',
        'cumulative_messages_dropped': 'sum',
        'cumulative_dht_queries': 'sum',
        'cumulative_dht_query_timeouts': 'sum'
    }).reset_index()
    
    # Cumulative messages (network-wide totals)
    axes[0].plot(aggregated['timestamp'], aggregated['cumulative_messages_received'], 
                 label='Total Received (Network)', linewidth=1.0, color='#06A77D')
    axes[0].plot(aggregated['timestamp'], aggregated['cumulative_messages_dropped'], 
                 label='Total Dropped (Network)', linewidth=1.0, color='#D00000', linestyle='--')
    axes[0].set_ylabel('Message Count', fontsize=11)
    axes[0].set_title('Network-Wide Cumulative Message Traffic', fontsize=12, fontweight='bold')
    axes[0].legend(loc='best', frameon=True, fancybox=True, shadow=True)
    axes[0].grid(True, alpha=0.3, linestyle='--')
    
    # Cumulative DHT queries (network-wide totals)
    axes[1].plot(aggregated['timestamp'], aggregated['cumulative_dht_queries'], 
                 label='Total Queries (Network)', linewidth=1.0, color='#118AB2')
    axes[1].plot(aggregated['timestamp'], aggregated['cumulative_dht_query_timeouts'], 
                 label='Total Timeouts (Network)', linewidth=1.0, color='#EF476F', linestyle='--')
    axes[1].set_xlabel('Time', fontsize=11)
    axes[1].set_ylabel('Query Count', fontsize=11)
    axes[1].set_title('Network-Wide Cumulative DHT Activity', fontsize=12, fontweight='bold')
    axes[1].legend(loc='best', frameon=True, fancybox=True, shadow=True)
    axes[1].grid(True, alpha=0.3, linestyle='--')
    
    # Format x-axis
    axes[1].xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    axes[1].xaxis.set_major_locator(mdates.MinuteLocator(interval=max(1, len(aggregated) // 10)))
    plt.setp(axes[1].xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'overview_cumulative_metrics.png'), format='png', dpi=300)
    plt.close()
    print(f"✓ Generated overview_cumulative_metrics.png")

def generate_overview_summary_stats(df, node_names, output_dir):
    """Generate summary statistics for the entire network."""
    stats = []
    stats.append("=" * 60)
    stats.append("D-LOCKSS Network-Wide Summary Statistics")
    stats.append("=" * 60)
    stats.append(f"\nNodes Analyzed: {len(node_names)}")
    stats.append(f"Nodes: {', '.join(sorted(node_names))}")
    
    # Get time range
    stats.append(f"\nTime Range:")
    stats.append(f"  Start: {df['timestamp'].min()}")
    stats.append(f"  End: {df['timestamp'].max()}")
    stats.append(f"  Duration: {(df['timestamp'].max() - df['timestamp'].min()).total_seconds():.2f} seconds")
    
    # Aggregate final values
    final_values = df.groupby('node').last()
    
    stats.append(f"\n--- Network-Wide Storage Metrics ---")
    stats.append(f"Total Pinned Files (Sum): {final_values['pinned_files'].sum()}")
    stats.append(f"Total Known Files (Sum): {final_values['known_files'].sum()}")
    stats.append(f"Average Pinned per Node: {final_values['pinned_files'].mean():.2f}")
    stats.append(f"Average Known per Node: {final_values['known_files'].mean():.2f}")
    
    stats.append(f"\n--- Network-Wide Network Metrics ---")
    stats.append(f"Total Messages Received: {final_values['cumulative_messages_received'].sum()}")
    stats.append(f"Total Messages Dropped: {final_values['cumulative_messages_dropped'].sum()}")
    if 'active_peers' in final_values.columns:
        stats.append(f"Max Active Peers (Any Node): {final_values['active_peers'].max()}")
    elif 'rate_limited_peers' in final_values.columns:
        stats.append(f"Max Active Peers (Any Node): {final_values['rate_limited_peers'].max()}")
    
    stats.append(f"\n--- Network-Wide Replication Metrics ---")
    stats.append(f"Total Replication Checks: {final_values['cumulative_replication_checks'].sum()}")
    stats.append(f"Total Success: {final_values['cumulative_replication_success'].sum()}")
    stats.append(f"Total Failures: {final_values['cumulative_replication_failures'].sum()}")
    total_checks = final_values['cumulative_replication_checks'].sum()
    if total_checks > 0:
        success_rate = (final_values['cumulative_replication_success'].sum() / total_checks) * 100
        stats.append(f"Success Rate: {success_rate:.2f}%")
    
    # Convergence metrics (if available)
    if 'files_at_target_replication' in final_values.columns:
        stats.append(f"\n--- Network-Wide Convergence Metrics ---")
        stats.append(f"Final Files at Target Replication: {final_values['files_at_target_replication'].sum()}")
        if 'avg_replication_level' in final_values.columns:
            stats.append(f"Final Average Replication Level: {final_values['avg_replication_level'].mean():.2f}")
        if 'files_converged_total' in final_values.columns:
            stats.append(f"Total Files Converged: {final_values['files_converged_total'].sum()}")
    
    stats.append(f"\n--- Network-Wide DHT Metrics ---")
    stats.append(f"Total DHT Queries: {final_values['cumulative_dht_queries'].sum()}")
    stats.append(f"Total DHT Timeouts: {final_values['cumulative_dht_query_timeouts'].sum()}")
    total_queries = final_values['cumulative_dht_queries'].sum()
    if total_queries > 0:
        timeout_rate = (final_values['cumulative_dht_query_timeouts'].sum() / total_queries) * 100
        stats.append(f"Timeout Rate: {timeout_rate:.2f}%")
    
    stats.append(f"\n--- Network-Wide Performance Metrics ---")
    stats.append(f"Total Worker Pool Active: {final_values['worker_pool_active'].sum()}")
    stats.append(f"Total Files in Backoff: {final_values['files_in_backoff'].sum()}")
    stats.append(f"Total Shard Splits: {final_values['cumulative_shard_splits'].sum()}")
    
    stats.append("\n" + "=" * 60)
    
    with open(os.path.join(output_dir, 'overview_summary_stats.txt'), 'w') as f:
        f.write('\n'.join(stats))
    
    print(f"✓ Generated overview_summary_stats.txt")

def resolve_search_dir(search_dir):
    """Resolve search directory, trying multiple locations."""
    # Try as-is (absolute or relative to current directory)
    path = Path(search_dir)
    if path.exists():
        return str(path.absolute())
    
    # Try relative to script's parent directory (project root)
    script_dir = Path(__file__).parent.parent
    alt_path = script_dir / search_dir
    if alt_path.exists():
        return str(alt_path.absolute())
    
    # Try relative to current working directory
    cwd_path = Path.cwd() / search_dir
    if cwd_path.exists():
        return str(cwd_path.absolute())
    
    # Return original if nothing found (will show error later)
    return search_dir

def main():
    parser = argparse.ArgumentParser(
        description='Generate scientific paper-quality charts from D-LOCKSS metrics CSV',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process a specific CSV file
  %(prog)s testnet/testnet_data/node_1/metrics.csv
  
  # Auto-discover and process all CSV files
  %(prog)s --auto
  
  # Auto-discover with custom search directory
  %(prog)s --auto --search-dir testnet/testnet_data
        """
    )
    parser.add_argument('csv_file', nargs='?', help='Path to metrics CSV file (optional if --auto is used)')
    parser.add_argument('output_dir', nargs='?', help='Output directory for charts (default: auto-generated timestamped directory)')
    parser.add_argument('--format', choices=['pdf', 'png', 'both'], default='both', help='Output format (default: both)')
    parser.add_argument('--auto', action='store_true', help='Automatically find and process all metrics.csv files in testnet directories')
    parser.add_argument('--search-dir', default='testnet/testnet_data', help='Directory to search for CSV files when using --auto (default: testnet/testnet_data). Can be relative or absolute path.')
    parser.add_argument('--base-output', default='charts', help='Base directory for output (default: charts/)')
    
    args = parser.parse_args()
    
    # Resolve search directory to absolute path
    resolved_search_dir = resolve_search_dir(args.search_dir)
    
    # Auto-discovery mode
    if args.auto or (not args.csv_file and Path(resolved_search_dir).exists()):
        print("Auto-discovery mode: Searching for metrics CSV files...")
        print(f"Search directory: {resolved_search_dir}")
        csv_files = find_metrics_csvs(resolved_search_dir)
        
        if not csv_files:
            print(f"\nError: No metrics.csv files found in {resolved_search_dir}")
            print("\nTroubleshooting:")
            print(f"  1. Check if the directory exists: ls -la {resolved_search_dir}")
            print(f"  2. Check for CSV files: find {resolved_search_dir} -name 'metrics.csv'")
            print("  3. Make sure you've run a testnet with metrics export enabled")
            print("  4. Try specifying the search directory explicitly:")
            print(f"     --search-dir /full/path/to/testnet_data")
            print(f"\nSearched in: {resolved_search_dir}")
            if resolved_search_dir != args.search_dir:
                print(f"  (resolved from: {args.search_dir})")
            sys.exit(1)
        
        print(f"Found {len(csv_files)} CSV file(s)")
        
        # Create timestamped output directory
        output_base_dir = create_timestamped_output_dir(args.base_output)
        print(f"\nOutput directory: {output_base_dir}")
        
        # Load all metrics and create overview charts
        print(f"\nLoading metrics from all {len(csv_files)} nodes...")
        combined_df, node_names = load_all_metrics(csv_files)
        
        if combined_df is None or len(combined_df) == 0:
            print("Error: Failed to load any metrics data")
            sys.exit(1)
        
        print(f"\n{'='*60}")
        print(f"Generating Network-Wide Overview Charts")
        print(f"Nodes: {len(node_names)}")
        print(f"Total Data Points: {len(combined_df)}")
        print(f"Output: {output_base_dir}/")
        print(f"{'='*60}")
        
        # Close any existing figures
        plt.close('all')
        
        # Generate overview charts
        plot_overview_storage_metrics(combined_df, output_base_dir)
        plt.close('all')
        
        plot_overview_network_metrics(combined_df, output_base_dir)
        plt.close('all')
        
        plot_overview_replication_metrics(combined_df, output_base_dir)
        plt.close('all')
        
        plot_overview_convergence_metrics(combined_df, output_base_dir)
        plt.close('all')
        
        plot_overview_replication_distribution(combined_df, output_base_dir)
        plt.close('all')
        
        plot_overview_performance_metrics(combined_df, output_base_dir)
        plt.close('all')
        
        plot_overview_cumulative_metrics(combined_df, output_base_dir)
        plt.close('all')
        
        generate_overview_summary_stats(combined_df, node_names, output_base_dir)
        
        print(f"\n{'='*60}")
        print(f"✓ Network-wide overview charts generated successfully")
        print(f"All charts saved to: {output_base_dir}/")
        print(f"{'='*60}")
        return
    
    # Single file mode
    if not args.csv_file:
        parser.print_help()
        sys.exit(1)
    
    if not os.path.exists(args.csv_file):
        print(f"Error: CSV file not found: {args.csv_file}")
        sys.exit(1)
    
    # Determine output directory
    if args.output_dir:
        output_dir = args.output_dir
    else:
        # Create timestamped directory based on CSV file location
        csv_path = Path(args.csv_file)
        node_name = csv_path.parent.name
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_dir = os.path.join(args.base_output, f'{node_name}_{timestamp}')
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"Loading metrics from {args.csv_file}...")
    df = load_metrics(args.csv_file)
    print(f"Loaded {len(df)} data points")
    
    print(f"\nGenerating charts in {output_dir}...")
    plot_storage_metrics(df, output_dir)
    plot_network_metrics(df, output_dir)
    plot_replication_metrics(df, output_dir)
    plot_performance_metrics(df, output_dir)
    plot_cumulative_metrics(df, output_dir)
    generate_summary_stats(df, output_dir)
    
    print(f"\n✓ All charts generated successfully in {output_dir}/")

if __name__ == '__main__':
    main()
