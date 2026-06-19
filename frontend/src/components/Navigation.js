import React, { useState } from 'react';
import { User, Settings, TrendingUp, BarChart3, Sparkles, Menu, X, Moon, Sun, FlaskConical } from 'lucide-react';

const Navigation = ({ 
  currentUser, 
  onUserChange, 
  onRefresh, 
  loading,
  stats,
  darkMode,
  onDarkModeToggle,
  activeTab,
  onTabChange,
}) => {
  const [showMobileMenu, setShowMobileMenu] = useState(false);
  const [showUserMenu, setShowUserMenu] = useState(false);

  const tabs = [
    { id: 'recommendations', label: 'For You', icon: Sparkles },
    { id: 'search', label: 'Search', icon: TrendingUp },
    { id: 'profile', label: 'Profile', icon: User },
    { id: 'experiments', label: 'A/B Tests', icon: FlaskConical },
  ];

  return (
    <nav className="glass-nav sticky top-0 z-40">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex justify-between items-center h-16">
          
          {/* Logo */}
          <div className="flex items-center">
            <div className="flex items-center space-x-3">
              <div className="bg-gradient-to-br from-blue-500 to-purple-600 p-2 rounded-xl shadow-lg glow-blue">
                <Sparkles className="text-white" size={22} />
              </div>
              <div>
                <h1 className="text-xl font-bold gradient-text">RecommendAI</h1>
                <span className="text-xs text-muted-foreground hidden sm:block">Intelligent Recommendation System</span>
              </div>
            </div>
          </div>

          {/* Desktop Tabs */}
          <div className="hidden md:flex items-center bg-muted/50 rounded-xl p-1 gap-0.5">
            {tabs.map(({ id, label, icon: Icon }) => (
              <button
                key={id}
                onClick={() => onTabChange(id)}
                className={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-sm font-medium transition-all duration-200 ${
                  activeTab === id
                    ? 'bg-primary text-primary-foreground shadow-sm'
                    : 'text-muted-foreground hover:text-foreground hover:bg-background/50'
                }`}
              >
                <Icon size={14} />
                {label}
              </button>
            ))}
          </div>

          {/* Right Controls */}
          <div className="hidden md:flex items-center space-x-3">
            {/* Stats pill */}
            {stats && (
              <div className="flex items-center space-x-3 text-xs text-muted-foreground bg-muted/50 px-3 py-1.5 rounded-xl">
                <span className="flex items-center gap-1">
                  <span className="live-dot w-1.5 h-1.5 rounded-full bg-green-400" />
                  {stats.total_users?.toLocaleString() || 0} users
                </span>
                <span>·</span>
                <span>{stats.total_items?.toLocaleString() || 0} items</span>
              </div>
            )}

            {/* User input */}
            <div className="flex items-center space-x-2">
              <div className="relative">
                <input
                  type="text"
                  value={currentUser}
                  onChange={(e) => onUserChange(e.target.value)}
                  className="pl-3 pr-8 py-1.5 bg-muted border border-border rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-primary/50 focus:border-primary transition-all duration-200 w-28 text-foreground placeholder:text-muted-foreground"
                  placeholder="User ID"
                />
                <User size={13} className="absolute right-2 top-1/2 -translate-y-1/2 text-muted-foreground" />
              </div>
            </div>

            {/* Refresh */}
            <button
              onClick={onRefresh}
              disabled={loading}
              className="bg-gradient-to-r from-blue-600 to-purple-600 hover:from-blue-700 hover:to-purple-700 disabled:opacity-50 text-white px-4 py-1.5 rounded-lg text-sm font-medium transition-all duration-200 flex items-center space-x-1.5 hover:scale-105 disabled:scale-100 shadow-md"
            >
              <TrendingUp size={14} className={loading ? 'animate-spin' : ''} />
              <span>{loading ? 'Loading...' : 'Refresh'}</span>
            </button>

            {/* Dark mode toggle */}
            <button
              onClick={onDarkModeToggle}
              aria-label={darkMode ? 'Switch to light mode' : 'Switch to dark mode'}
              className="p-2 rounded-lg bg-muted hover:bg-muted/80 text-muted-foreground hover:text-foreground transition-colors"
            >
              {darkMode ? <Sun size={16} /> : <Moon size={16} />}
            </button>

            {/* Settings dropdown */}
            <div className="relative">
              <button
                onClick={() => setShowUserMenu(!showUserMenu)}
                className="p-2 rounded-lg bg-muted hover:bg-muted/80 text-muted-foreground transition-colors"
                aria-label="Settings"
              >
                <Settings size={16} />
              </button>
              {showUserMenu && (
                <div className="absolute right-0 mt-2 w-56 glass-card rounded-xl shadow-xl py-2 z-50 border border-border">
                  <div className="px-4 py-2 text-xs text-muted-foreground border-b border-border">
                    Logged in as <span className="font-semibold text-foreground">{currentUser}</span>
                  </div>
                  <a
                    href={`${process.env.REACT_APP_BACKEND_URL || 'http://localhost:8000'}/api/user/${currentUser}/profile`}
                    target="_blank"
                    rel="noreferrer"
                    className="w-full text-left px-4 py-2 text-sm text-foreground hover:bg-muted flex items-center gap-2"
                  >
                    <User size={13} className="text-muted-foreground" />
                    View API Profile
                  </a>
                  <a
                    href={`${process.env.REACT_APP_BACKEND_URL || 'http://localhost:8000'}/metrics`}
                    target="_blank"
                    rel="noreferrer"
                    className="w-full text-left px-4 py-2 text-sm text-foreground hover:bg-muted flex items-center gap-2"
                  >
                    <BarChart3 size={13} className="text-muted-foreground" />
                    Prometheus Metrics
                  </a>
                </div>
              )}
            </div>
          </div>

          {/* Mobile menu button */}
          <div className="md:hidden flex items-center gap-2">
            <button
              onClick={onDarkModeToggle}
              className="p-2 rounded-lg bg-muted text-muted-foreground"
              aria-label="Toggle dark mode"
            >
              {darkMode ? <Sun size={16} /> : <Moon size={16} />}
            </button>
            <button
              onClick={() => setShowMobileMenu(!showMobileMenu)}
              className="text-muted-foreground hover:text-foreground p-2"
              aria-label="Toggle menu"
            >
              {showMobileMenu ? <X size={22} /> : <Menu size={22} />}
            </button>
          </div>
        </div>

        {/* Mobile Menu */}
        {showMobileMenu && (
          <div className="md:hidden border-t border-border py-4 space-y-3">
            {/* Tabs */}
            <div className="grid grid-cols-2 gap-2">
              {tabs.map(({ id, label, icon: Icon }) => (
                <button
                  key={id}
                  onClick={() => { onTabChange(id); setShowMobileMenu(false); }}
                  className={`flex items-center justify-center gap-2 px-3 py-2 rounded-xl text-sm font-medium transition-all ${
                    activeTab === id
                      ? 'bg-primary text-primary-foreground'
                      : 'bg-muted text-muted-foreground'
                  }`}
                >
                  <Icon size={14} />
                  {label}
                </button>
              ))}
            </div>

            {/* User input + Refresh */}
            <div className="flex gap-2">
              <input
                type="text"
                value={currentUser}
                onChange={(e) => onUserChange(e.target.value)}
                className="flex-1 px-3 py-2 bg-muted border border-border rounded-lg text-sm text-foreground focus:outline-none focus:ring-2 focus:ring-primary/50"
                placeholder="User ID"
              />
              <button
                onClick={onRefresh}
                disabled={loading}
                className="bg-gradient-to-r from-blue-600 to-purple-600 disabled:opacity-50 text-white px-4 py-2 rounded-lg text-sm font-medium"
              >
                {loading ? '...' : 'Go'}
              </button>
            </div>
          </div>
        )}
      </div>

      {/* Click outside to close menus */}
      {(showUserMenu || showMobileMenu) && (
        <div
          className="fixed inset-0 z-30"
          onClick={() => { setShowUserMenu(false); setShowMobileMenu(false); }}
        />
      )}
    </nav>
  );
};

export default Navigation;