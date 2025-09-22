import React from 'react';
import { X, Heart, Share2, Bookmark, ShoppingCart, Play, Eye, Star, TrendingUp, Clock, User } from 'lucide-react';

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

const ItemModal = ({ item, isOpen, onClose, onInteraction }) => {
  if (!isOpen || !item) return null;

  const contentTypeClass = CONTENT_TYPE_COLORS[item.content_type] || 'bg-gray-100 text-gray-800 border-gray-200';
  const icon = CONTENT_TYPE_ICONS[item.content_type] || '📄';

  const handleBackdropClick = (e) => {
    if (e.target === e.currentTarget) {
      onClose();
    }
  };

  const handleAction = (actionType) => {
    const context = { 
      source: 'modal',
      action: actionType,
      item_title: item.title,
      content_type: item.content_type
    };
    
    if (actionType === 'view') {
      context.view_duration = Math.floor(Math.random() * 600) + 60; // 1-10 minutes
    }
    
    onInteraction(item.item_id, actionType, context);
    
    // Close modal after certain actions
    if (['purchase', 'bookmark'].includes(actionType)) {
      setTimeout(() => onClose(), 1500);
    }
  };

  const formatNumber = (num) => {
    if (num >= 1000000) return (num / 1000000).toFixed(1) + 'M';
    if (num >= 1000) return (num / 1000).toFixed(1) + 'K';
    return num?.toString() || '0';
  };

  const getMainAction = () => {
    switch (item.content_type) {
      case 'video':
      case 'movie':
        return { icon: Play, label: 'Watch Now', action: 'view', color: 'from-red-500 to-red-600' };
      case 'product':
        return { icon: ShoppingCart, label: 'Purchase', action: 'purchase', color: 'from-green-500 to-green-600' };
      case 'article':
        return { icon: Eye, label: 'Read Article', action: 'view', color: 'from-blue-500 to-blue-600' };
      case 'course':
        return { icon: Play, label: 'Start Learning', action: 'view', color: 'from-orange-500 to-orange-600' };
      case 'music':
      case 'podcast':
        return { icon: Play, label: 'Play Now', action: 'view', color: 'from-purple-500 to-purple-600' };
      case 'game':
        return { icon: Play, label: 'Play Game', action: 'view', color: 'from-pink-500 to-pink-600' };
      default:
        return { icon: Eye, label: 'View', action: 'view', color: 'from-gray-500 to-gray-600' };
    }
  };

  const MainActionIcon = getMainAction().icon;

  return (
    <div 
      className="fixed inset-0 bg-black bg-opacity-75 flex items-center justify-center z-50 p-4 animate-fade-in"
      onClick={handleBackdropClick}
    >
      <div className="bg-white rounded-2xl max-w-4xl w-full max-h-[90vh] overflow-hidden shadow-2xl animate-slide-up">
        {/* Header with Image */}
        <div className="relative">
          <img 
            src={item.thumbnail_url} 
            alt={item.title}
            className="w-full h-64 md:h-80 object-cover"
          />
          
          {/* Gradient Overlay */}
          <div className="absolute inset-0 bg-gradient-to-t from-black/60 via-transparent to-transparent"></div>
          
          {/* Close Button */}
          <button
            onClick={onClose}
            className="absolute top-4 right-4 bg-black bg-opacity-50 hover:bg-opacity-70 text-white rounded-full p-2 transition-all duration-200 transform hover:scale-110"
          >
            <X size={20} />
          </button>
          
          {/* Content Type Badge */}
          <div className="absolute top-4 left-4">
            <span className={`inline-flex items-center px-3 py-1 rounded-full text-sm font-medium border backdrop-blur-sm ${contentTypeClass}`}>
              {icon} {item.content_type}
            </span>
          </div>
          
          {/* Bottom Info Overlay */}
          <div className="absolute bottom-4 left-4 right-4">
            <div className="flex items-center justify-between">
              <div className="text-white">
                <h2 className="text-2xl md:text-3xl font-bold mb-2 leading-tight">{item.title}</h2>
                <div className="flex items-center space-x-4 text-sm">
                  <span className="flex items-center">
                    <Star size={16} className="text-yellow-400 mr-1" />
                    {item.rating}
                  </span>
                  <span className="flex items-center">
                    <Eye size={16} className="mr-1" />
                    {formatNumber(item.view_count)}
                  </span>
                  <span className="bg-blue-600 text-white px-2 py-1 rounded-full text-xs font-medium">
                    {item.category}
                  </span>
                </div>
              </div>
              
              {/* Scores */}
              <div className="flex flex-col space-y-2">
                {item.ml_score > 0 && (
                  <div className="bg-green-500 text-white px-3 py-1 rounded-full text-xs font-medium flex items-center">
                    <TrendingUp size={12} className="mr-1" />
                    ML: {item.ml_score.toFixed(2)}
                  </div>
                )}
                {item.search_score > 0 && (
                  <div className="bg-purple-500 text-white px-3 py-1 rounded-full text-xs font-medium">
                    🔍 {item.search_score.toFixed(2)}
                  </div>
                )}
              </div>
            </div>
          </div>
        </div>

        {/* Content */}
        <div className="p-6 max-h-96 overflow-y-auto">
          {/* Description */}
          <div className="mb-6">
            <h3 className="text-lg font-semibold text-gray-900 mb-3">Description</h3>
            <p className="text-gray-700 leading-relaxed">{item.description}</p>
          </div>

          {/* Tags */}
          {item.tags && item.tags.length > 0 && (
            <div className="mb-6">
              <h3 className="text-lg font-semibold text-gray-900 mb-3">Tags</h3>
              <div className="flex flex-wrap gap-2">
                {item.tags.map((tag, index) => (
                  <span 
                    key={index}
                    className="inline-block bg-gray-100 hover:bg-blue-50 hover:text-blue-600 text-gray-700 text-sm px-3 py-1 rounded-full transition-all duration-200 cursor-pointer transform hover:scale-105"
                  >
                    #{tag}
                  </span>
                ))}
              </div>
            </div>
          )}

          {/* Stats Grid */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
            <div className="bg-blue-50 p-3 rounded-lg text-center">
              <Star className="text-blue-600 mx-auto mb-1" size={20} />
              <div className="text-lg font-bold text-blue-900">{item.rating}</div>
              <div className="text-xs text-blue-600">Rating</div>
            </div>
            
            <div className="bg-green-50 p-3 rounded-lg text-center">
              <Eye className="text-green-600 mx-auto mb-1" size={20} />
              <div className="text-lg font-bold text-green-900">{formatNumber(item.view_count)}</div>
              <div className="text-xs text-green-600">Views</div>
            </div>
            
            <div className="bg-purple-50 p-3 rounded-lg text-center">
              <User className="text-purple-600 mx-auto mb-1" size={20} />
              <div className="text-lg font-bold text-purple-900">{item.category}</div>
              <div className="text-xs text-purple-600">Category</div>
            </div>
            
            <div className="bg-orange-50 p-3 rounded-lg text-center">
              <Clock className="text-orange-600 mx-auto mb-1" size={20} />
              <div className="text-lg font-bold text-orange-900">{item.content_type}</div>
              <div className="text-xs text-orange-600">Type</div>
            </div>
          </div>

          {/* Action Buttons */}
          <div className="flex flex-wrap gap-3">
            {/* Main Action */}
            <button
              onClick={() => handleAction(getMainAction().action)}
              className={`flex-1 min-w-[200px] bg-gradient-to-r ${getMainAction().color} hover:shadow-lg text-white px-6 py-3 rounded-xl font-semibold transition-all duration-200 flex items-center justify-center space-x-2 transform hover:scale-105`}
            >
              <MainActionIcon size={20} />
              <span>{getMainAction().label}</span>
            </button>
            
            {/* Secondary Actions */}
            <button
              onClick={() => handleAction('like')}
              className="bg-red-500 hover:bg-red-600 text-white px-6 py-3 rounded-xl font-semibold transition-all duration-200 flex items-center space-x-2 transform hover:scale-105"
            >
              <Heart size={20} />
              <span className="hidden sm:inline">Like</span>
            </button>
            
            <button
              onClick={() => handleAction('bookmark')}
              className="bg-yellow-500 hover:bg-yellow-600 text-white px-6 py-3 rounded-xl font-semibold transition-all duration-200 flex items-center space-x-2 transform hover:scale-105"
            >
              <Bookmark size={20} />
              <span className="hidden sm:inline">Save</span>
            </button>
            
            <button
              onClick={() => handleAction('share')}
              className="bg-blue-500 hover:bg-blue-600 text-white px-6 py-3 rounded-xl font-semibold transition-all duration-200 flex items-center space-x-2 transform hover:scale-105"
            >
              <Share2 size={20} />
              <span className="hidden sm:inline">Share</span>
            </button>
          </div>
        </div>
      </div>
    </div>
  );
};

export default ItemModal;