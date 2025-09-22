import React from 'react';
import { Users, Package, Activity, TrendingUp, Eye, Heart, Share, BarChart3, Clock } from 'lucide-react';

const StatsDashboard = ({ stats, abTestInfo, className = "" }) => {
  if (!stats) return null;

  const statCards = [
    { 
      title: 'Total Users', 
      value: stats.total_users, 
      color: 'blue', 
      icon: Users,
      description: 'Registered users'
    },
    { 
      title: 'Content Items', 
      value: stats.total_items, 
      color: 'green', 
      icon: Package,
      description: 'Available content'
    },
    { 
      title: 'Active Users', 
      value: stats.active_users_24h, 
      color: 'purple', 
      icon: Activity,
      description: 'Last 24 hours'
    },
    { 
      title: 'Interactions', 
      value: stats.interactions_24h, 
      color: 'orange', 
      icon: TrendingUp,
      description: 'Last 24 hours'
    }
  ];

  const formatNumber = (num) => {
    if (num >= 1000000) return (num / 1000000).toFixed(1) + 'M';
    if (num >= 1000) return (num / 1000).toFixed(1) + 'K';
    return num?.toLocaleString() || '0';
  };

  const getColorClasses = (color) => ({
    blue: {
      bg: 'bg-blue-50',
      border: 'border-blue-200',
      text: 'text-blue-900',
      subtext: 'text-blue-700',
      icon: 'text-blue-600'
    },
    green: {
      bg: 'bg-green-50',
      border: 'border-green-200',
      text: 'text-green-900',
      subtext: 'text-green-700',
      icon: 'text-green-600'
    },
    purple: {
      bg: 'bg-purple-50',
      border: 'border-purple-200',
      text: 'text-purple-900',
      subtext: 'text-purple-700',
      icon: 'text-purple-600'
    },
    orange: {
      bg: 'bg-orange-50',
      border: 'border-orange-200',
      text: 'text-orange-900',
      subtext: 'text-orange-700',
      icon: 'text-orange-600'
    }
  }[color]);

  return (
    <div className={`bg-white rounded-xl shadow-lg border border-gray-100 overflow-hidden ${className}`}>
      {/* Header */}
      <div className="bg-gradient-to-r from-blue-50 to-purple-50 px-6 py-4 border-b border-gray-100">
        <div className="flex items-center justify-between">
          <h2 className="text-xl font-bold text-gray-900 flex items-center">
            <BarChart3 className="mr-3 text-blue-600" size={24} />
            System Analytics
          </h2>
          <div className="flex items-center space-x-2 text-sm text-gray-600">
            <Clock size={16} />
            <span>Live Data</span>
            <div className="w-2 h-2 bg-green-500 rounded-full animate-pulse"></div>
          </div>
        </div>
      </div>
      
      {/* Stats Grid */}
      <div className="p-6">
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
          {statCards.map((card, index) => {
            const colors = getColorClasses(card.color);
            const IconComponent = card.icon;
            
            return (
              <div 
                key={index} 
                className={`${colors.bg} ${colors.border} border p-4 rounded-xl hover:shadow-md transition-all duration-200 transform hover:-translate-y-1`}
              >
                <div className="flex items-center justify-between mb-3">
                  <IconComponent className={`${colors.icon}`} size={24} />
                  <span className="text-xs font-medium bg-white px-2 py-1 rounded-full text-gray-600 shadow-sm">
                    LIVE
                  </span>
                </div>
                <h3 className={`text-sm font-medium ${colors.subtext} mb-1`}>
                  {card.title}
                </h3>
                <p className={`text-2xl font-bold ${colors.text} mb-1`}>
                  {formatNumber(card.value)}
                </p>
                <p className="text-xs text-gray-500">
                  {card.description}
                </p>
              </div>
            );
          })}
        </div>

        {/* Content Type Breakdown */}
        {stats.items_by_type && Object.keys(stats.items_by_type).length > 0 && (
          <div className="mb-6">
            <h3 className="text-lg font-semibold text-gray-900 mb-3 flex items-center">
              <Package className="mr-2" size={20} />
              Content Distribution
            </h3>
            <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
              {Object.entries(stats.items_by_type).map(([type, count]) => {
                const icons = {
                  video: '🎥', movie: '🍿', article: '📰', product: '🛍️',
                  music: '🎵', podcast: '🎧', course: '📚', game: '🎮'
                };
                
                return (
                  <div key={type} className="bg-gray-50 p-3 rounded-lg text-center hover:bg-gray-100 transition-colors">
                    <div className="text-2xl mb-1">{icons[type] || '📄'}</div>
                    <div className="text-sm font-medium text-gray-700 capitalize">{type}</div>
                    <div className="text-lg font-bold text-gray-900">{formatNumber(count)}</div>
                  </div>
                );
              })}
            </div>
          </div>
        )}

        {/* Popular Categories */}
        {stats.popular_categories && stats.popular_categories.length > 0 && (
          <div className="mb-6">
            <h3 className="text-lg font-semibold text-gray-900 mb-3 flex items-center">
              <TrendingUp className="mr-2" size={20} />
              Trending Categories (7 days)
            </h3>
            <div className="space-y-2">
              {stats.popular_categories.slice(0, 5).map((category, index) => (
                <div key={index} className="flex items-center justify-between p-3 bg-gray-50 rounded-lg hover:bg-gray-100 transition-colors">
                  <div className="flex items-center space-x-3">
                    <span className="text-sm font-medium text-gray-500">#{index + 1}</span>
                    <span className="font-medium text-gray-900">{category._id}</span>
                  </div>
                  <div className="flex items-center space-x-2">
                    <span className="text-sm text-gray-600">{formatNumber(category.interaction_count)} interactions</span>
                    <div className="w-16 bg-gray-200 rounded-full h-2">
                      <div 
                        className="bg-gradient-to-r from-blue-500 to-purple-500 h-2 rounded-full transition-all duration-500"
                        style={{
                          width: `${Math.min((category.interaction_count / Math.max(...stats.popular_categories.map(c => c.interaction_count))) * 100, 100)}%`
                        }}
                      ></div>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* A/B Test Information */}
        {abTestInfo && (
          <div className="bg-gradient-to-r from-gray-50 to-blue-50 p-4 rounded-xl border border-gray-200">
            <h3 className="text-lg font-semibold text-gray-800 mb-3 flex items-center">
              <div className="bg-blue-100 p-1 rounded-lg mr-2">
                🧪
              </div>
              A/B Test Status
            </h3>
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
              <div className="space-y-2">
                <div className="flex items-center justify-between">
                  <span className="text-sm text-gray-600">Current Algorithm:</span>
                  <span className={`px-3 py-1 rounded-full text-sm font-medium ${
                    abTestInfo.arm === 'xgboost_ml' 
                      ? 'bg-green-100 text-green-800' 
                      : 'bg-blue-100 text-blue-800'
                  }`}>
                    {abTestInfo.arm === 'xgboost_ml' ? '🤖 ML Algorithm' : '📊 Popularity Based'}
                  </span>
                </div>
                <div className="flex items-center justify-between">
                  <span className="text-sm text-gray-600">Experiment:</span>
                  <span className="text-sm font-medium text-gray-800">
                    {abTestInfo.experiment_name || 'ML vs Popularity'}
                  </span>
                </div>
              </div>
              <div className="bg-white p-3 rounded-lg border border-gray-200">
                <div className="text-xs text-gray-500 mb-1">Test Bucket</div>
                <div className="text-lg font-bold text-gray-900">{abTestInfo.bucket}</div>
                <div className="text-xs text-gray-500">
                  Assigned: {new Date(abTestInfo.timestamp).toLocaleTimeString()}
                </div>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default StatsDashboard;