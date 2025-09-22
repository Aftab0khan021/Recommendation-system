import hashlib
import logging
from datetime import datetime
from typing import Dict, Any
from collections import defaultdict

logger = logging.getLogger(__name__)

class ABTestManager:
    def __init__(self):
        self.experiments = {
            'recommendation_algorithm_v1': {
                'name': 'ML vs Popularity Algorithm Test',
                'description': 'Comparing XGBoost ML recommendations vs popularity-based recommendations',
                'traffic_split': 0.5,  # 50% traffic to treatment (ML)
                'control_arm': 'popularity_based',
                'treatment_arm': 'xgboost_ml',
                'start_date': datetime.utcnow(),
                'status': 'active'
            }
        }
        self.user_assignments = {}  # Cache user assignments
        self.experiment_events = defaultdict(list)
    
    def get_user_bucket(self, user_id: str, experiment_id: str) -> str:
        """Deterministically assign user to experiment bucket"""
        if user_id in self.user_assignments and experiment_id in self.user_assignments[user_id]:
            return self.user_assignments[user_id][experiment_id]
        
        # Use hash of user_id + experiment_id for consistent assignment
        hash_input = f"{user_id}_{experiment_id}".encode('utf-8')
        hash_value = int(hashlib.md5(hash_input).hexdigest(), 16)
        
        experiment = self.experiments.get(experiment_id)
        if not experiment:
            return 'control'
        
        # Assign based on traffic split
        bucket = 'treatment' if (hash_value % 100) < (experiment['traffic_split'] * 100) else 'control'
        
        # Cache assignment
        if user_id not in self.user_assignments:
            self.user_assignments[user_id] = {}
        self.user_assignments[user_id][experiment_id] = bucket
        
        return bucket
    
    def should_use_xgboost(self, user_id: str) -> bool:
        """Determine if user should get XGBoost recommendations"""
        bucket = self.get_user_bucket(user_id, 'recommendation_algorithm_v1')
        return bucket == 'treatment'
    
    def get_ab_test_info(self, user_id: str, experiment_id: str) -> Dict[str, Any]:
        """Get A/B test information for user"""
        bucket = self.get_user_bucket(user_id, experiment_id)
        experiment = self.experiments.get(experiment_id, {})
        
        if bucket == 'treatment':
            arm = experiment.get('treatment_arm', 'treatment')
        else:
            arm = experiment.get('control_arm', 'control')
        
        return {
            'user_id': user_id,
            'experiment_id': experiment_id,
            'bucket': bucket,
            'arm': arm,
            'experiment_name': experiment.get('name', 'Unknown Experiment'),
            'timestamp': datetime.utcnow()
        }
    
    def log_experiment_event(self, user_id: str, experiment_id: str, event_type: str, event_data: Dict[str, Any] = None):
        """Log experiment event for analysis"""
        try:
            bucket = self.get_user_bucket(user_id, experiment_id)
            
            event = {
                'timestamp': datetime.utcnow(),
                'user_id': user_id,
                'experiment_id': experiment_id,
                'bucket': bucket,
                'event_type': event_type,
                'event_data': event_data or {}
            }
            
            self.experiment_events[experiment_id].append(event)
            
            # Keep only recent events (last 1000 per experiment)
            if len(self.experiment_events[experiment_id]) > 1000:
                self.experiment_events[experiment_id] = self.experiment_events[experiment_id][-1000:]
                
        except Exception as e:
            logger.error(f"Error logging experiment event: {e}")
    
    def get_experiment_metrics(self, experiment_id: str) -> Dict[str, Any]:
        """Get experiment performance metrics"""
        try:
            events = self.experiment_events.get(experiment_id, [])
            
            if not events:
                return {"error": "No events found for experiment"}
            
            # Group events by bucket
            control_events = [e for e in events if e['bucket'] == 'control']
            treatment_events = [e for e in events if e['bucket'] == 'treatment']
            
            # Calculate basic metrics
            metrics = {
                'experiment_id': experiment_id,
                'total_events': len(events),
                'control_events': len(control_events),
                'treatment_events': len(treatment_events),
                'control_users': len(set([e['user_id'] for e in control_events])),
                'treatment_users': len(set([e['user_id'] for e in treatment_events])),
            }
            
            # Calculate interaction rates
            control_interactions = len([e for e in control_events if e['event_type'].startswith('interaction_')])
            treatment_interactions = len([e for e in treatment_events if e['event_type'].startswith('interaction_')])
            
            control_requests = len([e for e in control_events if e['event_type'] == 'recommendation_request'])
            treatment_requests = len([e for e in treatment_events if e['event_type'] == 'recommendation_request'])
            
            if control_requests > 0:
                metrics['control_interaction_rate'] = control_interactions / control_requests
            else:
                metrics['control_interaction_rate'] = 0
                
            if treatment_requests > 0:
                metrics['treatment_interaction_rate'] = treatment_interactions / treatment_requests
            else:
                metrics['treatment_interaction_rate'] = 0
            
            return metrics
            
        except Exception as e:
            logger.error(f"Error calculating experiment metrics: {e}")
            return {"error": str(e)}
    
    def get_all_experiments(self) -> Dict[str, Any]:
        """Get all experiment configurations and metrics"""
        result = {}
        
        for exp_id, exp_config in self.experiments.items():
            result[exp_id] = {
                'config': exp_config,
                'metrics': self.get_experiment_metrics(exp_id)
            }
        
        return result

# Global A/B test manager instance
ab_test_manager = ABTestManager()