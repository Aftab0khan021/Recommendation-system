import React, { useState } from 'react';
import { User, Settings, TrendingUp, BarChart3, Sparkles, Menu, X } from 'lucide-react';

const Navigation = ({ 
  currentUser, 
  onUserChange, 
  onRefresh, 
  loading,
  stats 
}) => {
  const [showMobileMenu, setShowMobileMenu] = useState(false);
  const [showUserMenu, setShowUserMenu] = useState(false);

  return (
    <nav className="bg-white shadow-lg sticky top-0 z-40 border-b border-gray-100">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex justify-between items-center h-16">
          {/* Logo and Brand */}
          <div className="flex items-center">
            <div className="flex items-center space-x-3">
              <div className="bg-gradient-to-r from-blue-500 to-purple-600 p-2 rounded-xl shadow-lg">
                <Sparkles className="text-white" size={24} />
              </div>
              <div>
                <h1 className="text-2xl font-bold bg-gradient-to-r from-blue-600 to-purple-600 bg-clip-text text-transparent">
                  RecommendAI
                </h1>
                <span className="text-xs text-gray-500 hidden sm:block">Intelligent Recommendation System</span>
              </div>
            </div>
          </div>
          
          {/* Desktop Navigation */}
          <div className="hidden md:flex items-center space-x-6">
            {/* Stats Summary */}
            {stats && (
              <div className="flex items-center space-x-4 text-sm text-gray-600">
                <div className="flex items-center space-x-1">
                  <User size={16} />
                  <span>{stats.total_users?.toLocaleString() || 0} users</span>
                </div>
                <div className="flex items-center space-x-1">
                  <BarChart3 size={16} />
                  <span>{stats.total_items?.toLocaleString() || 0} items</span>
                </div>
                <div className="flex items-center space-x-1">
                  <TrendingUp size={16} />
                  <span>{stats.active_users_24h?.toLocaleString() || 0} active</span>
                </div>
              </div>
            )}

            {/* User Input */}
            <div className="flex items-center space-x-2">
              <label className="text-sm font-medium text-gray-700">User:</label>
              <div className="relative">
                <input
                  type="text"
                  value={currentUser}
                  onChange={(e) => onUserChange(e.target.value)}
                  className="px-3 py-2 border border-gray-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all duration-200 w-32"
                  placeholder="User ID"
                />
                <User size={16} className="absolute right-2 top-1/2 transform -translate-y-1/2 text-gray-400" />
              </div>
            </div>

            {/* Refresh Button */}
            <button
              onClick={onRefresh}
              disabled={loading}
              className="bg-blue-600 hover:bg-blue-700 disabled:bg-blue-300 text-white px-4 py-2 rounded-lg transition-all duration-200 flex items-center space-x-2 transform hover:scale-105 disabled:transform-none"
            >
              <TrendingUp size={16} className={loading ? 'animate-spin' : ''} />
              <span className="hidden lg:inline">{loading ? 'Loading...' : 'Refresh'}</span>
            </button>

            {/* User Menu */}
            <div className="relative">
              <button
                onClick={() => setShowUserMenu(!showUserMenu)}
                className="bg-gray-100 hover:bg-gray-200 text-gray-600 p-2 rounded-lg transition-colors duration-200"
              >
                <Settings size={18} />
              </button>

              {showUserMenu && (
                <div className="absolute right-0 mt-2 w-48 bg-white rounded-lg shadow-xl border border-gray-200 py-2 z-50">
                  <div className="px-4 py-2 text-sm text-gray-600 border-b border-gray-100">
                    Current User: {currentUser}
                  </div>
                  <button className="w-full text-left px-4 py-2 text-sm text-gray-700 hover:bg-gray-50">
                    View Profile
                  </button>
                  <button className="w-full text-left px-4 py-2 text-sm text-gray-700 hover:bg-gray-50">
                    Preferences
                  </button>
                  <button className="w-full text-left px-4 py-2 text-sm text-gray-700 hover:bg-gray-50">
                    Analytics
                  </button>
                </div>
              )}
            </div>
          </div>

          {/* Mobile Menu Button */}
          <div className="md:hidden">
            <button
              onClick={() => setShowMobileMenu(!showMobileMenu)}
              className="text-gray-600 hover:text-gray-800 p-2"
            >
              {showMobileMenu ? <X size={24} /> : <Menu size={24} />}
            </button>
          </div>
        </div>

        {/* Mobile Menu */}
        {showMobileMenu && (
          <div className="md:hidden border-t border-gray-200 py-4">
            <div className="space-y-4">
              {/* User Input */}
              <div className="flex items-center space-x-2">
                <label className="text-sm font-medium text-gray-700">User:</label>
                <input
                  type="text"
                  value={currentUser}
                  onChange={(e) => onUserChange(e.target.value)}
                  className="flex-1 px-3 py-2 border border-gray-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                  placeholder="Enter user ID"
                />
              </div>

              {/* Stats */}
              {stats && (
                <div className="grid grid-cols-3 gap-2 text-sm text-gray-600">
                  <div className="flex items-center space-x-1">
                    <User size={14} />
                    <span>{stats.total_users?.toLocaleString() || 0}</span>
                  </div>
                  <div className="flex items-center space-x-1">
                    <BarChart3 size={14} />
                    <span>{stats.total_items?.toLocaleString() || 0}</span>
                  </div>
                  <div className="flex items-center space-x-1">
                    <TrendingUp size={14} />
                    <span>{stats.active_users_24h?.toLocaleString() || 0}</span>
                  </div>
                </div>
              )}

              {/* Actions */}
              <div className="flex space-x-2">
                <button
                  onClick={onRefresh}
                  disabled={loading}
                  className="flex-1 bg-blue-600 hover:bg-blue-700 disabled:bg-blue-300 text-white px-4 py-2 rounded-lg transition-all duration-200 flex items-center justify-center space-x-2"
                >
                  <TrendingUp size={16} className={loading ? 'animate-spin' : ''} />
                  <span>{loading ? 'Loading...' : 'Refresh'}</span>
                </button>
                
                <button className="bg-gray-100 hover:bg-gray-200 text-gray-600 px-4 py-2 rounded-lg transition-colors duration-200">
                  <Settings size={16} />
                </button>
              </div>
            </div>
          </div>
        )}
      </div>

      {/* Click outside to close menus */}
      {(showUserMenu || showMobileMenu) && (
        <div 
          className="fixed inset-0 z-30" 
          onClick={() => {
            setShowUserMenu(false);
            setShowMobileMenu(false);
          }}
        />
      )}
    </nav>
  );
};

export default Navigation;