import React, { useEffect, useState } from 'react';
import axios from 'axios';
import { FlaskConical, Users, TrendingUp, CheckCircle, XCircle, BarChart3, Percent } from 'lucide-react';

const BACKEND_URL = process.env.REACT_APP_BACKEND_URL || 'http://127.0.0.1:8000';

/**
 * Phase 2+3: A/B Test Analytics Dashboard
 * Shows experiment metrics, statistical significance, and bandit stats.
 */

const MetricPill = ({ label, value, highlight = false }) => (
  <div className={`rounded-xl p-3 text-center ${highlight ? 'bg-primary/10 border border-primary/20' : 'bg-muted/50'}`}>
    <p className="text-xs text-muted-foreground uppercase tracking-wide mb-1">{label}</p>
    <p className={`text-lg font-bold ${highlight ? 'text-primary' : 'text-foreground'}`}>{value ?? '—'}</p>
  </div>
);

const BarComparison = ({ label1, value1, label2, value2, color1 = '#3b82f6', color2 = '#8b5cf6' }) => {
  const max = Math.max(value1 || 0, value2 || 0, 0.01);
  return (
    <div className="space-y-2">
      <div>
        <div className="flex justify-between text-xs mb-1">
          <span className="text-muted-foreground">{label1}</span>
          <span className="font-semibold text-foreground">{(value1 * 100).toFixed(1)}%</span>
        </div>
        <div className="h-3 rounded-full bg-muted overflow-hidden">
          <div className="h-3 rounded-full transition-all duration-700" style={{ width: `${(value1/max)*100}%`, background: color1 }} />
        </div>
      </div>
      <div>
        <div className="flex justify-between text-xs mb-1">
          <span className="text-muted-foreground">{label2}</span>
          <span className="font-semibold text-foreground">{(value2 * 100).toFixed(1)}%</span>
        </div>
        <div className="h-3 rounded-full bg-muted overflow-hidden">
          <div className="h-3 rounded-full transition-all duration-700" style={{ width: `${(value2/max)*100}%`, background: color2 }} />
        </div>
      </div>
    </div>
  );
};

const ExperimentsPage = ({ userId }) => {
  const [experiments, setExperiments] = useState(null);
  const [banditStats, setBanditStats] = useState(null);
  const [loading, setLoading] = useState(true);

  const fetchData = async () => {
    try {
      const [expRes, banditRes] = await Promise.allSettled([
        axios.get(`${BACKEND_URL}/api/ab/experiments`),
        axios.get(`${BACKEND_URL}/api/ab/bandit-stats/recommendation_algorithm_v1`),
      ]);
      if (expRes.status === 'fulfilled') setExperiments(expRes.value.data);
      if (banditRes.status === 'fulfilled') setBanditStats(banditRes.value.data);
    } catch (e) {
      console.warn('Could not load experiment data:', e);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { fetchData(); }, []);

  if (loading) {
    return (
      <div className="space-y-4 page-enter">
        {[1, 2].map(i => (
          <div key={i} className="glass-card rounded-2xl p-6 h-48 skeleton-shimmer" />
        ))}
      </div>
    );
  }

  const expData = experiments?.recommendation_algorithm_v1;
  const metrics = expData?.metrics;
  const config = expData?.config;

  return (
    <div className="space-y-6 page-enter">
      {/* Header */}
      <div className="glass-card rounded-2xl p-6">
        <div className="flex items-center gap-3 mb-2">
          <div className="p-2 rounded-xl bg-primary/10">
            <FlaskConical size={20} className="text-primary" />
          </div>
          <div>
            <h2 className="text-xl font-bold text-foreground">
              {config?.name || 'A/B Test Dashboard'}
            </h2>
            <p className="text-sm text-muted-foreground">{config?.description}</p>
          </div>
          <div className="ml-auto">
            <span className="px-3 py-1 rounded-full text-xs font-medium bg-green-500/10 text-green-500 border border-green-500/20">
              ● Active
            </span>
          </div>
        </div>
        <div className="flex gap-4 mt-4 text-sm text-muted-foreground">
          <span>Traffic split: <strong className="text-foreground">{Math.round((config?.traffic_split || 0.5) * 100)}% treatment</strong></span>
          <span>•</span>
          <span>Control: <strong className="text-foreground">{config?.control_arm}</strong></span>
          <span>•</span>
          <span>Treatment: <strong className="text-foreground">{config?.treatment_arm}</strong></span>
        </div>
      </div>

      {/* Metrics */}
      {metrics && !metrics.message && (
        <>
          {/* Event counts */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            <MetricPill label="Total Events" value={metrics.total_events} />
            <MetricPill label="Control Users" value={metrics.control_users} />
            <MetricPill label="Treatment Users" value={metrics.treatment_users} highlight />
            <MetricPill label="Total Users" value={(metrics.control_users || 0) + (metrics.treatment_users || 0)} />
          </div>

          {/* CTR Comparison */}
          <div className="glass-card rounded-2xl p-6">
            <h3 className="text-base font-semibold text-foreground mb-4 flex items-center gap-2">
              <BarChart3 size={16} className="text-primary" />
              Interaction Rate Comparison
            </h3>
            <BarComparison
              label1="Control (Popularity-based)"
              value1={metrics.control_interaction_rate || 0}
              label2="Treatment (XGBoost ML)"
              value2={metrics.treatment_interaction_rate || 0}
            />
          </div>

          {/* Statistical Significance */}
          <div className="glass-card rounded-2xl p-6">
            <h3 className="text-base font-semibold text-foreground mb-4 flex items-center gap-2">
              <Percent size={16} className="text-secondary" />
              Statistical Significance
            </h3>

            {metrics.p_value !== undefined && metrics.p_value !== null ? (
              <div className="space-y-4">
                <div className={`flex items-center gap-3 p-4 rounded-xl ${
                  metrics.is_significant
                    ? 'bg-green-500/10 border border-green-500/30'
                    : 'bg-muted border border-border'
                }`}>
                  {metrics.is_significant
                    ? <CheckCircle size={20} className="text-green-500" />
                    : <XCircle size={20} className="text-muted-foreground" />
                  }
                  <div>
                    <p className={`font-semibold ${metrics.is_significant ? 'text-green-500' : 'text-foreground'}`}>
                      {metrics.confidence_level}
                    </p>
                    {metrics.lift_pct !== null && (
                      <p className="text-sm text-muted-foreground">
                        Lift: <strong className="text-foreground">
                          {metrics.lift_pct > 0 ? '+' : ''}{metrics.lift_pct}%
                        </strong>
                      </p>
                    )}
                  </div>
                </div>
                <div className="grid grid-cols-3 gap-3">
                  <MetricPill label="Z-Score" value={metrics.z_score?.toFixed(3)} />
                  <MetricPill label="P-Value" value={metrics.p_value?.toFixed(4)} />
                  <MetricPill label="Lift" value={metrics.lift_pct ? `${metrics.lift_pct > 0 ? '+' : ''}${metrics.lift_pct}%` : '—'} highlight />
                </div>
              </div>
            ) : (
              <p className="text-muted-foreground text-sm bg-muted rounded-xl p-4">
                {metrics.confidence_level || 'Need more data for significance testing.'}
              </p>
            )}
          </div>
        </>
      )}

      {metrics?.message && (
        <div className="glass-card rounded-2xl p-8 text-center">
          <FlaskConical size={48} className="mx-auto text-muted-foreground mb-4" />
          <p className="text-foreground font-medium">No experiment data yet</p>
          <p className="text-muted-foreground text-sm mt-1">{metrics.message}</p>
        </div>
      )}

      {/* Thompson Sampling Bandit Stats */}
      {banditStats && (
        <div className="glass-card rounded-2xl p-6">
          <h3 className="text-base font-semibold text-foreground mb-1 flex items-center gap-2">
            <TrendingUp size={16} className="text-accent" />
            Thompson Sampling Bandit
          </h3>
          <p className="text-xs text-muted-foreground mb-4">
            Mode: <strong>{banditStats.mode}</strong> — automatically shifts traffic toward better arm
          </p>
          <div className="grid grid-cols-2 gap-4">
            {Object.entries(banditStats.arms || {}).map(([arm, stats]) => (
              <div key={arm} className="bg-muted/50 rounded-xl p-4 border border-border">
                <div className="flex items-center justify-between mb-2">
                  <p className="text-sm font-semibold text-foreground capitalize">{arm}</p>
                  <span className="text-xs bg-primary/10 text-primary px-2 py-0.5 rounded-full">
                    {stats.traffic_pct}% traffic
                  </span>
                </div>
                <p className="text-2xl font-bold text-foreground">{(stats.estimated_ctr * 100).toFixed(1)}%</p>
                <p className="text-xs text-muted-foreground mt-1">
                  Est. CTR • {stats.observations} obs
                </p>
                <div className="h-1.5 rounded-full bg-muted mt-2 overflow-hidden">
                  <div className="h-1.5 rounded-full bg-gradient-to-r from-primary to-secondary"
                    style={{ width: `${stats.traffic_pct}%` }} />
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Current user info */}
      {userId && (
        <div className="glass-card rounded-2xl p-4 flex items-center gap-3 text-sm">
          <Users size={16} className="text-muted-foreground" />
          <span className="text-muted-foreground">
            You (<strong className="text-foreground">{userId}</strong>) are in the{' '}
            <strong className="text-foreground">recommendation_algorithm_v1</strong> experiment
          </span>
        </div>
      )}
    </div>
  );
};

export default ExperimentsPage;
