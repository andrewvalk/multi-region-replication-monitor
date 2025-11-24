#!/usr/bin/env python3
"""
Multi-Region Database Replication Monitor
Monitors replication lag and health across regions
"""

import psycopg2
import time
from datetime import datetime
from typing import Dict, List
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


class ReplicationMonitor:
    
    def __init__(self):
        self.regions = {
            'us-west': {'host': 'localhost', 'port': 5441, 'role': 'primary'},
            'us-east': {'host': 'localhost', 'port': 5442, 'role': 'replica'},
            'eu-west': {'host': 'localhost', 'port': 5443, 'role': 'replica'}
        }
        self.connections = {}
        self.metrics_history = []
        self.alerts = []
        
    def connect_all(self):
        logger.info("Connecting to all regions...")
        
        for region, config in self.regions.items():
            try:
                conn = psycopg2.connect(
                    host=config['host'],
                    port=config['port'],
                    dbname='replication_db',
                    user='postgres',
                    password='postgres'
                )
                conn.autocommit = True
                self.connections[region] = conn
                logger.info(f"Connected to {region} ({config['role']})")
            except Exception as e:
                logger.error(f"Failed to connect to {region}: {e}")
        
        return len(self.connections) == len(self.regions)
    
    def setup_primary(self):
        logger.info("Setting up primary database...")
        
        conn = self.connections['us-west']
        cursor = conn.cursor()
        
        cursor.execute("""
            DROP TABLE IF EXISTS replication_test;
            DROP TABLE IF EXISTS transactions;
            
            CREATE TABLE replication_test (
                id SERIAL PRIMARY KEY,
                region VARCHAR(50),
                timestamp TIMESTAMP DEFAULT NOW(),
                data TEXT
            );
            
            CREATE TABLE transactions (
                txn_id SERIAL PRIMARY KEY,
                amount DECIMAL(10,2),
                created_at TIMESTAMP DEFAULT NOW()
            );
            
            INSERT INTO transactions (amount)
            SELECT (random() * 1000)::DECIMAL(10,2)
            FROM generate_series(1, 1000);
        """)
        
        cursor.close()
        logger.info("Primary database initialized with test data")
    
    def simulate_writes(self, count: int = 10):
        """Simulate writes to primary"""
        conn = self.connections['us-west']
        cursor = conn.cursor()
        
        for i in range(count):
            cursor.execute(
                "INSERT INTO replication_test (region, data) VALUES (%s, %s)",
                ('us-west', f'Test data {i} at {datetime.now()}')
            )
        
        cursor.close()
        logger.info(f"Wrote {count} records to primary")
    
    def check_replication_status(self) -> Dict:
        """Check replication status for all replicas"""
        
        primary_conn = self.connections['us-west']
        primary_cursor = primary_conn.cursor()
        
        primary_cursor.execute("SELECT COUNT(*) FROM replication_test")
        primary_count = primary_cursor.fetchone()[0]
        
        primary_cursor.execute("SELECT MAX(id) FROM replication_test")
        primary_max_id = primary_cursor.fetchone()[0] or 0
        
        primary_cursor.close()
        
        status = {
            'timestamp': datetime.now().isoformat(),
            'primary': {
                'region': 'us-west',
                'record_count': primary_count,
                'max_id': primary_max_id
            },
            'replicas': []
        }
        
        for region in ['us-east', 'eu-west']:
            replica_conn = self.connections[region]
            replica_cursor = replica_conn.cursor()
            
            try:
                replica_cursor.execute("SELECT COUNT(*) FROM replication_test")
                replica_count = replica_cursor.fetchone()[0]
                
                replica_cursor.execute("SELECT MAX(id) FROM replication_test")
                replica_max_id = replica_cursor.fetchone()[0] or 0
                
                lag_records = primary_count - replica_count
                lag_ids = primary_max_id - replica_max_id
                
                replica_status = {
                    'region': region,
                    'record_count': replica_count,
                    'max_id': replica_max_id,
                    'lag_records': lag_records,
                    'lag_ids': lag_ids,
                    'is_healthy': lag_records < 10
                }
                
                status['replicas'].append(replica_status)
                
                if not replica_status['is_healthy']:
                    self.create_alert(region, lag_records)
                
            except Exception as e:
                logger.error(f"Error checking {region}: {e}")
                status['replicas'].append({
                    'region': region,
                    'error': str(e),
                    'is_healthy': False
                })
            
            replica_cursor.close()
        
        self.metrics_history.append(status)
        return status
    
    def create_alert(self, region: str, lag: int):
        """Create alert for high replication lag"""
        alert = {
            'timestamp': datetime.now().isoformat(),
            'region': region,
            'severity': 'WARNING' if lag < 50 else 'CRITICAL',
            'message': f'High replication lag detected: {lag} records behind',
            'lag_count': lag
        }
        self.alerts.append(alert)
        logger.warning(f"ALERT: {region} has {lag} records lag")
    
    def print_status(self, status: Dict):
        """Print formatted replication status"""
        
        print("\n" + "=" * 80)
        print(f"MULTI-REGION REPLICATION STATUS - {status['timestamp'][:19]}")
        print("=" * 80)
        
        print(f"\nPRIMARY (us-west):")
        print(f"  Total Records: {status['primary']['record_count']:,}")
        print(f"  Max ID: {status['primary']['max_id']:,}")
        
        print(f"\nREPLICAS:")
        for replica in status['replicas']:
            if 'error' in replica:
                print(f"\n  {replica['region'].upper()}: ERROR")
                print(f"    {replica['error']}")
            else:
                health = "HEALTHY" if replica['is_healthy'] else "LAGGING"
                print(f"\n  {replica['region'].upper()}: {health}")
                print(f"    Records: {replica['record_count']:,}")
                print(f"    Max ID: {replica['max_id']:,}")
                print(f"    Lag: {replica['lag_records']} records")
                
                if replica['lag_records'] > 0:
                    print(f"    Status: Behind by {replica['lag_records']} records")
        
        if self.alerts:
            recent_alerts = [a for a in self.alerts if a['timestamp'] > status['timestamp'][:19]]
            if recent_alerts:
                print(f"\nACTIVE ALERTS: {len(recent_alerts)}")
        
        print("=" * 80)
    
    def run_monitoring(self, duration: int = 30):
        """Run continuous monitoring"""
        
        print("\n" + "=" * 80)
        print("MULTI-REGION REPLICATION MONITOR")
        print("=" * 80)
        
        if not self.connect_all():
            logger.error("Failed to connect to all regions")
            return
        
        self.setup_primary()
        
        logger.info("Waiting for initial replication...")
        time.sleep(3)
        
        start_time = time.time()
        iteration = 0
        
        while time.time() - start_time < duration:
            iteration += 1
            
            if iteration % 2 == 0:
                self.simulate_writes(5)
            
            status = self.check_replication_status()
            
            if iteration % 2 == 0:
                self.print_status(status)
            
            time.sleep(5)
        
        print("\n" + "=" * 80)
        print("MONITORING SUMMARY")
        print("=" * 80)
        print(f"Duration: {duration}s")
        print(f"Metrics Collected: {len(self.metrics_history)}")
        print(f"Total Alerts: {len(self.alerts)}")
        
        if self.alerts:
            print("\nAlert Summary:")
            for alert in self.alerts[-5:]:
                print(f"  [{alert['timestamp'][:19]}] {alert['region']}: {alert['message']}")
        
        print("=" * 80)


def main():
    monitor = ReplicationMonitor()
    monitor.run_monitoring(duration=30)


if __name__ == "__main__":
    main()
