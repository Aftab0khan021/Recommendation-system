import React from 'react';
import { Heart, Eye, Share2, Bookmark, ShoppingCart, Play, Clock, Star, TrendingUp } from 'lucide-react';

const CONTENT_TYPE_ICONS = {
  video: '🎥',
  movie: '🍿',
  article: '📰',
  product: '🛍️',
  music: '🎵',
  podcast: '🎧',
  course: '📚',
  game: '🎮'
};

const CONTENT_TYPE_COLORS = {
  video: 'bg-red-100 text-red-800 border-red-200',
  movie: 'bg-purple-100 text-purple-800 border-purple-200',
  article: 'bg-blue-100 text-blue-800 border-blue-200',
  product: 'bg-green-100 text-green-800 border-green-200',
  music: 'bg-yellow-100 text-yellow-800 border-yellow-200',
  podcast: 'bg-indigo-100 text-indigo-800 border-indigo-200',
  course: 'bg-orange-100 text-orange-800 border-orange-200',
  game: 'bg-pink-100 text-pink-800 border-pink-200'
};

const RecommendationCard = ({ item, onInteraction, onClick, className = '', showScore = false }) => {
  const contentTypeClass = CONTENT_TYPE_COLORS[item.content_type] || 'bg-gray-100 text-gray-800 border-gray-200';
  const icon = CONTENT_TYPE_ICONS[item.content_type] || '📄';

  const handleClick = (e) => {
    e.preventDefault();
    onInteraction(item.item_id, 'click', { source: 'recommendation_card' });
    onClick(item);
  };

  const handleQuickAction = (e, action) => {
    e.stopPropagation();
    const context = { 
      source: 'quick_action',
      action: action,
      item_title: item.title
    };
    
    if (action === 'view') {
      context.view_duration = Math.floor(Math.random() * 300) + 30;
    }
    
    onInteraction(item.item_id, action, context);
  };

  const formatNumber = (num) => {
    if (num >= 1000000) return (num / 1000000).toFixed(1) + 'M';
    if (num >= 1000) return (num / 1000).toFixed(1) + 'K';
    return num?.toString() || '0';
  };

  const getActionButton = () => {
    switch (item.content_type) {
      case 'video':
      case 'movie':
        return { icon: Play, label: 'Watch', action: 'view' };
      case 'product':
        return { icon: ShoppingCart, label: 'Buy', action: 'purchase' };
      case 'article':
        return { icon: Eye, label: 'Read', action: 'view' };
      case 'course':
        return { icon: Play, label: 'Learn', action: 'view' };
      case 'music':
      case 'podcast':
        return { icon: Play, label: 'Listen', action: 'view' };
      default:
        return { icon: Eye, label: 'View', action: 'view' };
    }
  };

  const ActionIcon = getActionButton().icon;

  return (
    <div 
      className={`bg-white rounded-xl shadow-md hover:shadow-xl transition-all duration-300 cursor-pointer transform hover:-translate-y-1 border border-gray-100 overflow-hidden group ${className}`}
      onClick={handleClick}
    >
      {/* Image Container */}
      <div className="relative overflow-hidden aspect-video">
        <img 
          src={item.thumbnail_url} 
          alt={item.title}
          className="w-full h-full object-cover group-hover:scale-105 transition-transform duration-300"
          loading="lazy"
        />
        
        {/* Overlay on hover */}
        <div className="absolute inset-0 bg-black bg-opacity-0 group-hover:bg-opacity-20 transition-all duration-300 flex items-center justify-center">
          <div className="opacity-0 group-hover:opacity-100 transition-opacity duration-300">
            <div className="bg-white bg-opacity-90 rounded-full p-3 transform scale-0 group-hover:scale-100 transition-transform duration-300">
              <ActionIcon size={24} className="text-gray-800" />
            </div>
          </div>
        </div>
        
        {/* Content type badge */}
        <div className="absolute top-3 left-3">
          <span className={`inline-flex items-center px-2 py-1 rounded-lg text-xs font-semibold border backdrop-blur-sm ${contentTypeClass}`}>
            {icon} {item.content_type}
          </span>
        </div>
        
        {/* Quick actions */}
        <div className="absolute top-3 right-3 flex space-x-1 opacity-0 group-hover:opacity-100 transition-opacity duration-300">
          <button
            onClick={(e) => handleQuickAction(e, 'like')}
            className="bg-white bg-opacity-90 hover:bg-white rounded-full p-2 transition-all duration-200 transform hover:scale-110 shadow-lg"
            title="Like this item"
          >
            <Heart size={16} className="text-red-500" />
          </button>
          
          <button
            onClick={(e) => handleQuickAction(e, 'bookmark')}
            className="bg-white bg-opacity-90 hover:bg-white rounded-full p-2 transition-all duration-200 transform hover:scale-110 shadow-lg"
            title="Bookmark"
          >
            <Bookmark size={16} className="text-blue-500" />
          </button>
        </div>

        {/* ML Score Badge */}
        {showScore && item.ml_score > 0 && (
          <div className="absolute bottom-3 left-3">
            <span className="bg-green-500 text-white text-xs px-2 py-1 rounded-full font-medium flex items-center">
              <TrendingUp size={12} className="mr-1" />
              {item.ml_score.toFixed(2)}
            </span>
          </div>
        )}

        {/* Search Score Badge */}
        {showScore && item.search_score > 0 && (
          <div className="absolute bottom-3 right-3">
            <span className="bg-purple-500 text-white text-xs px-2 py-1 rounded-full font-medium">
              🔍 {item.search_score.toFixed(2)}
            </span>
          </div>
        )}
      </div>
      
      {/* Content */}
      <div className="p-4">
        <h3 className="text-lg font-bold text-gray-900 mb-2 line-clamp-2 group-hover:text-blue-600 transition-colors duration-200">
          {item.title}
        </h3>
        
        <p className="text-sm text-gray-600 mb-3 line-clamp-2">{item.description}</p>
        
        {/* Category and Stats */}
        <div className="flex items-center justify-between mb-3">
          <span className="text-sm font-semibold text-blue-600 bg-blue-50 px-2 py-1 rounded-lg">
            {item.category}
          </span>
          <div className="flex items-center space-x-3 text-sm text-gray-500">
            <span className="flex items-center">
              <Star size={14} className="text-yellow-500 mr-1" />
              {item.rating}
            </span>
            <span className="flex items-center">
              <Eye size={14} className="mr-1" />
              {formatNumber(item.view_count)}
            </span>
          </div>
        </div>
        
        {/* Tags */}
        {item.tags && item.tags.length > 0 && (
          <div className="flex flex-wrap gap-1 mb-3">
            {item.tags.slice(0, 3).map((tag, index) => (
              <span 
                key={index}
                className="inline-block bg-gray-100 hover:bg-gray-200 text-gray-700 text-xs px-2 py-1 rounded-full transition-colors duration-200 cursor-pointer"
              >
                #{tag}
              </span>
            ))}
            {item.tags.length > 3 && (
              <span className="text-xs text-gray-500 self-center">+{item.tags.length - 3} more</span>
            )}
          </div>
        )}

        {/* Action Button */}
        <div className="flex items-center space-x-2">
          <button
            onClick={(e) => handleQuickAction(e, getActionButton().action)}
            className="flex-1 bg-gradient-to-r from-blue-600 to-purple-600 hover:from-blue-700 hover:to-purple-700 text-white px-4 py-2 rounded-lg font-medium transition-all duration-200 flex items-center justify-center space-x-2 transform hover:scale-105"
          >
            <ActionIcon size={16} />
            <span>{getActionButton().label}</span>
          </button>
          
          <button
            onClick={(e) => handleQuickAction(e, 'share')}
            className="bg-gray-100 hover:bg-gray-200 text-gray-600 p-2 rounded-lg transition-colors duration-200"
            title="Share"
          >
            <Share2 size={16} />
          </button>
        </div>
      </div>
    </div>
  );
};

export default RecommendationCard;