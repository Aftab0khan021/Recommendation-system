import React from 'react';

/**
 * SkeletonCard — mirrors the exact layout of RecommendationCard.
 * Shows a shimmering pulse placeholder while real data loads.
 * Eliminates layout shift (CLS) and feels far more premium than a spinner.
 */
const SkeletonCard = () => (
  <div className="bg-white rounded-xl shadow-md border border-gray-100 overflow-hidden animate-pulse">
    {/* Image placeholder */}
    <div className="aspect-video bg-gradient-to-r from-gray-200 via-gray-100 to-gray-200 bg-[length:200%_100%] skeleton-shimmer" />

    <div className="p-4 space-y-3">
      {/* Title — two lines */}
      <div className="space-y-2">
        <div className="h-4 bg-gray-200 rounded-full w-full" />
        <div className="h-4 bg-gray-200 rounded-full w-3/4" />
      </div>

      {/* Description — two lines */}
      <div className="space-y-1.5">
        <div className="h-3 bg-gray-100 rounded-full w-full" />
        <div className="h-3 bg-gray-100 rounded-full w-5/6" />
      </div>

      {/* Category + stats row */}
      <div className="flex items-center justify-between">
        <div className="h-6 bg-blue-100 rounded-lg w-24" />
        <div className="flex items-center space-x-3">
          <div className="h-4 bg-gray-100 rounded-full w-10" />
          <div className="h-4 bg-gray-100 rounded-full w-10" />
        </div>
      </div>

      {/* Tags */}
      <div className="flex gap-1.5">
        <div className="h-5 bg-gray-100 rounded-full w-14" />
        <div className="h-5 bg-gray-100 rounded-full w-16" />
        <div className="h-5 bg-gray-100 rounded-full w-12" />
      </div>

      {/* Action button */}
      <div className="h-9 bg-gradient-to-r from-blue-200 to-purple-200 rounded-lg w-full" />
    </div>
  </div>
);

/**
 * SkeletonGrid — renders N skeleton cards in the same responsive grid
 * layout as the real recommendation grid.
 */
export const SkeletonGrid = ({ count = 8 }) => (
  <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-6 mb-12">
    {Array.from({ length: count }).map((_, i) => (
      <SkeletonCard key={i} />
    ))}
  </div>
);

export default SkeletonCard;
