import React, { useEffect, useState } from 'react';
import axios from 'axios';
import {
  User, Activity, Clock, Star, BarChart3, TrendingUp,
  Film, Headphones, BookOpen, Gamepad, Monitor, Newspaper,
  ShoppingBag, Music, Sparkles
} from 'lucide-react';

const BACKEND_URL = process.env.REACT_APP_BACKEND_URL || 'http://127.0.0.1:8000';

const CONTENT_TYPE_ICONS = {
  video: Monitor, movie: Film, article: Newspaper, product: ShoppingBag,
  music: Music, podcast: Headphones, course: BookOpen, game: Gamepad,
};

const StatCard = ({ icon: Icon, label, value, color = 'text-primary' }) => (
  <div className="glass-card rounded-2xl p-5 flex items-center gap-4">
    <div className={`p-3 rounded-xl bg-primary/10 ${color}`}>
      <Icon size={22} />
    </div>
    <div>
      <p className="text-xs text-muted-foreground font-medium uppercase tracking-wide">{label}</p>
      <p className="text-2xl font-bold text-foreground">{value ?? '—'}</p>
    </div>
  </div>
);

/**
 * Phase 2: User Profile Page
 * Shows stats, category affinities, and device info for the current user.
 */
const ProfilePage = ({ userId }) => {
  const [profile, setProfile] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (!userId) return;
    setLoading(true);
    axios.get(`${BACKEND_URL}/api/user/${userId}/profile`)
      .then(r => { setProfile(r.data); setError(null); })
      .catch(() => setError('Could not load profile.'))
      .finally(() => setLoading(false));
  }, [userId]);

  if (loading) {
    return (
      <div className="space-y-4 page-enter">
        {[1,2,3,4].map(i => (
          <div key={i} className="glass-card rounded-2xl p-5 h-24 skeleton-shimmer" />
        ))}
      </div>
    );
  }

  if (error || !profile) {
    return (
      <div className="glass-card rounded-2xl p-8 text-center page-enter">
        <User size={48} className="mx-auto text-muted-foreground mb-4" />
        <p className="text-foreground font-medium">No profile data yet</p>
        <p className="text-muted-foreground text-sm mt-1">
          Interact with some items to build your profile!
        </p>
      </div>
    );
  }

  const cats = Object.entries(profile.top_categories || {})
    .sort(([,a],[,b]) => b - a)
    .slice(0, 8);

  const maxCat = cats[0]?.[1] || 1;

  return (
    <div className="space-y-6 page-enter">
      {/* Header card */}
      <div className="glass-card rounded-2xl p-6 flex items-center gap-6">
        <div className="w-20 h-20 rounded-2xl bg-gradient-to-br from-primary to-secondary flex items-center justify-center glow-blue">
          <User size={36} className="text-white" />
        </div>
        <div>
          <h2 className="text-2xl font-bold text-foreground">{userId}</h2>
          <p className="text-muted-foreground text-sm flex items-center gap-2 mt-1">
            <span className="live-dot w-2 h-2 rounded-full bg-green-400 inline-block" />
            Active user
          </p>
          <div className="flex items-center gap-3 mt-2 text-xs text-muted-foreground">
            <span>{profile.country || 'US'} 🌎</span>
            <span>•</span>
            <span>{profile.device || 'web'} 💻</span>
            <span>•</span>
            <span>{profile.age_group || '25-34'} 👤</span>
          </div>
        </div>
      </div>

      {/* Stats grid */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <StatCard
          icon={Activity}
          label="Total Interactions"
          value={profile.total_interactions?.toLocaleString()}
        />
        <StatCard
          icon={TrendingUp}
          label="Recommendations Seen"
          value={profile.recommendations_served?.toLocaleString()}
          color="text-secondary"
        />
        <StatCard
          icon={Clock}
          label="Avg Dwell Time"
          value={profile.avg_dwell_time ? `${Math.round(profile.avg_dwell_time)}s` : '—'}
          color="text-accent"
        />
        <StatCard
          icon={Star}
          label="Avg Rating Given"
          value={profile.avg_rating ? profile.avg_rating.toFixed(1) : '—'}
          color="text-yellow-500"
        />
      </div>

      {/* Taste Profile — Category Affinities */}
      {cats.length > 0 && (
        <div className="glass-card rounded-2xl p-6">
          <h3 className="text-lg font-semibold text-foreground mb-4 flex items-center gap-2">
            <Sparkles size={18} className="text-primary" />
            Taste Profile
          </h3>
          <div className="space-y-3">
            {cats.map(([cat, count]) => {
              const pct = Math.round((count / maxCat) * 100);
              return (
                <div key={cat}>
                  <div className="flex items-center justify-between mb-1">
                    <span className="text-sm font-medium text-foreground capitalize">{cat}</span>
                    <span className="text-xs text-muted-foreground">{count} interactions</span>
                  </div>
                  <div className="h-2 rounded-full bg-muted overflow-hidden">
                    <div
                      className="h-2 rounded-full bg-gradient-to-r from-primary to-secondary transition-all duration-700"
                      style={{ width: `${pct}%` }}
                    />
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* Content Type Breakdown */}
      {profile.content_type_breakdown && (
        <div className="glass-card rounded-2xl p-6">
          <h3 className="text-lg font-semibold text-foreground mb-4 flex items-center gap-2">
            <BarChart3 size={18} className="text-secondary" />
            Content Preferences
          </h3>
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
            {Object.entries(profile.content_type_breakdown)
              .sort(([,a],[,b]) => b - a)
              .slice(0, 8)
              .map(([type, count]) => {
                const Icon = CONTENT_TYPE_ICONS[type] || Monitor;
                return (
                  <div key={type} className="bg-muted/50 rounded-xl p-3 text-center hover:bg-muted transition-colors">
                    <Icon size={24} className="mx-auto text-primary mb-1" />
                    <p className="text-xs font-medium text-foreground capitalize">{type}</p>
                    <p className="text-sm font-bold text-primary">{count}</p>
                  </div>
                );
              })}
          </div>
        </div>
      )}

      {/* A/B Test Bucket */}
      {profile.ab_bucket && (
        <div className="glass-card rounded-2xl p-4 flex items-center gap-4 border border-primary/20">
          <div className="p-2 rounded-lg bg-primary/10">
            <Activity size={16} className="text-primary" />
          </div>
          <div>
            <p className="text-xs text-muted-foreground">A/B Test Assignment</p>
            <p className="text-sm font-semibold text-foreground">
              {profile.ab_bucket === 'treatment'
                ? '🤖 XGBoost ML Recommendations'
                : '📊 Popularity-Based Recommendations'}
            </p>
          </div>
        </div>
      )}
    </div>
  );
};

export default ProfilePage;
