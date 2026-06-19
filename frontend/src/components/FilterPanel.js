import React, { useState, useCallback } from 'react';
import { X, SlidersHorizontal, ChevronDown, ChevronUp, RotateCcw } from 'lucide-react';

/**
 * Phase 2: Advanced Filter Panel
 * - Multi-select content type chips
 * - Sort options (Relevance, Rating, Most Viewed, Newest)
 * - Active filter badge count
 * - Collapsible sidebar design
 */

const CONTENT_TYPES = [
  { value: '', label: 'All Content', icon: '🌐' },
  { value: 'video',   label: 'Videos',   icon: '🎥' },
  { value: 'movie',   label: 'Movies',   icon: '🍿' },
  { value: 'article', label: 'Articles', icon: '📰' },
  { value: 'product', label: 'Products', icon: '🛍️' },
  { value: 'music',   label: 'Music',    icon: '🎵' },
  { value: 'podcast', label: 'Podcasts', icon: '🎧' },
  { value: 'course',  label: 'Courses',  icon: '📚' },
  { value: 'game',    label: 'Games',    icon: '🎮' },
];

const SORT_OPTIONS = [
  { value: 'relevance', label: 'Relevance',   icon: '✨' },
  { value: 'rating',    label: 'Top Rated',   icon: '⭐' },
  { value: 'views',     label: 'Most Viewed', icon: '👁️' },
  { value: 'newest',    label: 'Newest',      icon: '🆕' },
];

const FilterPanel = ({
  contentTypeFilter = '',
  onContentTypeChange,
  sortBy = 'relevance',
  onSortChange,
  isOpen,
  onClose,
}) => {
  const [showContentTypes, setShowContentTypes] = useState(true);
  const [showSort, setShowSort] = useState(true);

  const activeCount = (contentTypeFilter ? 1 : 0) + (sortBy !== 'relevance' ? 1 : 0);

  const handleReset = useCallback(() => {
    onContentTypeChange('');
    onSortChange('relevance');
  }, [onContentTypeChange, onSortChange]);

  if (!isOpen) return null;

  return (
    <>
      {/* Backdrop */}
      <div
        className="fixed inset-0 bg-black/20 dark:bg-black/50 z-30 md:hidden"
        onClick={onClose}
      />

      {/* Panel */}
      <aside className="fixed md:sticky top-0 md:top-20 left-0 md:left-auto h-screen md:h-auto w-72 md:w-64
        glass-card rounded-none md:rounded-2xl shadow-2xl z-40
        flex flex-col overflow-y-auto
        border-r md:border border-border
        bg-card dark:bg-card
        page-enter">

        {/* Header */}
        <div className="flex items-center justify-between p-4 border-b border-border">
          <div className="flex items-center gap-2">
            <SlidersHorizontal size={18} className="text-primary" />
            <h2 className="font-semibold text-foreground">Filters</h2>
            {activeCount > 0 && (
              <span className="bg-primary text-primary-foreground text-xs rounded-full w-5 h-5 flex items-center justify-center font-bold">
                {activeCount}
              </span>
            )}
          </div>
          <div className="flex items-center gap-2">
            {activeCount > 0 && (
              <button
                onClick={handleReset}
                className="text-xs text-muted-foreground hover:text-primary transition-colors flex items-center gap-1"
                title="Reset all filters"
              >
                <RotateCcw size={12} />
                Reset
              </button>
            )}
            <button
              onClick={onClose}
              className="md:hidden text-muted-foreground hover:text-foreground p-1 rounded-lg hover:bg-muted transition-colors"
            >
              <X size={16} />
            </button>
          </div>
        </div>

        {/* Content Type Filter */}
        <div className="p-4 border-b border-border">
          <button
            className="w-full flex items-center justify-between text-sm font-medium text-foreground mb-3"
            onClick={() => setShowContentTypes(!showContentTypes)}
          >
            Content Type
            {showContentTypes ? <ChevronUp size={14} className="text-muted-foreground" /> : <ChevronDown size={14} className="text-muted-foreground" />}
          </button>
          {showContentTypes && (
            <div className="flex flex-wrap gap-2">
              {CONTENT_TYPES.map(ct => (
                <button
                  key={ct.value}
                  onClick={() => onContentTypeChange(ct.value === contentTypeFilter ? '' : ct.value)}
                  className={`flex items-center gap-1 px-2.5 py-1.5 rounded-full text-xs font-medium transition-all duration-200 ${
                    contentTypeFilter === ct.value
                      ? 'bg-primary text-primary-foreground glow-blue'
                      : 'bg-muted text-muted-foreground hover:bg-muted/80 hover:text-foreground'
                  }`}
                >
                  <span>{ct.icon}</span>
                  <span>{ct.label}</span>
                </button>
              ))}
            </div>
          )}
        </div>

        {/* Sort Options */}
        <div className="p-4">
          <button
            className="w-full flex items-center justify-between text-sm font-medium text-foreground mb-3"
            onClick={() => setShowSort(!showSort)}
          >
            Sort By
            {showSort ? <ChevronUp size={14} className="text-muted-foreground" /> : <ChevronDown size={14} className="text-muted-foreground" />}
          </button>
          {showSort && (
            <div className="space-y-1.5">
              {SORT_OPTIONS.map(opt => (
                <button
                  key={opt.value}
                  onClick={() => onSortChange(opt.value)}
                  className={`w-full flex items-center gap-3 px-3 py-2 rounded-xl text-sm transition-all duration-200 ${
                    sortBy === opt.value
                      ? 'bg-primary/10 text-primary font-medium border border-primary/20'
                      : 'text-foreground hover:bg-muted'
                  }`}
                >
                  <span>{opt.icon}</span>
                  <span>{opt.label}</span>
                  {sortBy === opt.value && (
                    <div className="ml-auto w-2 h-2 rounded-full bg-primary" />
                  )}
                </button>
              ))}
            </div>
          )}
        </div>
      </aside>
    </>
  );
};

export default FilterPanel;
