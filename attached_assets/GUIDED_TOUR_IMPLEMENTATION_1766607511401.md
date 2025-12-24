# Guided Tour Dialog Component

This document contains the implementation details for the enhanced Guided Tour Dialog component.

## Dependencies

Ensure you have the following packages installed in your project:

```bash
npm install framer-motion lucide-react clsx tailwind-merge
```

## Component Implementation

Create a file named `guided-tour-dialog.tsx` in your components directory (e.g., `src/components/guided-tour-dialog.tsx`) and paste the following code:

```tsx
import React, { useState, useEffect } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { 
  ChevronLeft, 
  ChevronRight, 
  Play, 
  Pause, 
  X
} from "lucide-react";
import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";

// Utility for tailwind class merging
function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

// Button Component (Simplified version if you don't have one)
const Button = React.forwardRef<HTMLButtonElement, React.ButtonHTMLAttributes<HTMLButtonElement> & { variant?: 'default' | 'ghost' | 'outline', size?: 'default' | 'sm' | 'icon' }>(
  ({ className, variant = "default", size = "default", ...props }, ref) => {
    return (
      <button
        ref={ref}
        className={cn(
          "inline-flex items-center justify-center rounded-md text-sm font-medium ring-offset-background transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:pointer-events-none disabled:opacity-50",
          {
            "bg-primary text-primary-foreground hover:bg-primary/90": variant === "default",
            "hover:bg-accent hover:text-accent-foreground": variant === "ghost",
            "border border-input bg-background hover:bg-accent hover:text-accent-foreground": variant === "outline",
            "h-10 px-4 py-2": size === "default",
            "h-9 rounded-md px-3": size === "sm",
            "h-10 w-10": size === "icon",
          },
          className
        )}
        {...props}
      />
    )
  }
)
Button.displayName = "Button"

interface TourStep {
  title: string;
  content: string;
  target?: string;
}

interface GuidedTourDialogProps {
  steps: TourStep[];
  isOpen: boolean;
  onClose: () => void;
  className?: string;
}

export function GuidedTourDialog({ steps, isOpen, onClose, className }: GuidedTourDialogProps) {
  const [currentStep, setCurrentStep] = useState(0);
  const [isPlaying, setIsPlaying] = useState(false);

  // Auto-play logic
  useEffect(() => {
    let interval: NodeJS.Timeout;
    if (isPlaying) {
      interval = setInterval(() => {
        setCurrentStep((prev) => {
          if (prev < steps.length - 1) return prev + 1;
          setIsPlaying(false);
          return prev;
        });
      }, 3000);
    }
    return () => clearInterval(interval);
  }, [isPlaying, steps.length]);

  const handleNext = () => {
    if (currentStep < steps.length - 1) setCurrentStep(currentStep + 1);
  };

  const handlePrev = () => {
    if (currentStep > 0) setCurrentStep(currentStep - 1);
  };

  const step = steps[currentStep];

  return (
    <AnimatePresence>
      {isOpen && (
        <motion.div
          initial={{ opacity: 0, scale: 0.9, y: 20 }}
          animate={{ opacity: 1, scale: 1, y: 0 }}
          exit={{ opacity: 0, scale: 0.9, y: 20 }}
          transition={{ type: "spring", damping: 20, stiffness: 300 }}
          drag
          dragMomentum={false}
          whileDrag={{ scale: 1.02, cursor: "grabbing" }}
          className={cn(
            "fixed z-50 w-[400px] overflow-hidden rounded-xl border border-white/10",
            "bg-black/60 backdrop-blur-xl shadow-[0_20px_50px_rgba(0,0,0,0.5)]",
            "text-white font-sans",
            // 3D Tilt Effect visual cues
            "before:absolute before:inset-0 before:bg-gradient-to-br before:from-white/5 before:to-transparent before:pointer-events-none",
            "after:absolute after:inset-0 after:rounded-xl after:shadow-[inset_0_1px_1px_rgba(255,255,255,0.1)] after:pointer-events-none",
            className
          )}
          style={{
            left: "calc(50% - 200px)",
            top: "calc(50% - 150px)",
            perspective: "1000px",
          }}
        >
          {/* Header handle for dragging */}
          <div className="flex items-center justify-between px-4 py-3 border-b border-white/10 cursor-grab active:cursor-grabbing bg-white/5">
            <div className="flex items-center gap-2">
              <div className="w-2 h-2 rounded-full bg-cyan-400 shadow-[0_0_10px_rgba(34,211,238,0.5)] animate-pulse" />
              <span className="text-sm font-medium tracking-wide text-cyan-100/90">
                Guided Validation
              </span>
            </div>
            <div className="flex items-center gap-1">
              <button 
                onClick={onClose} 
                className="p-1 hover:bg-white/10 rounded-md transition-colors"
              >
                <X className="w-4 h-4 text-white/70" />
              </button>
            </div>
          </div>

          {/* Content */}
          <div className="p-6 relative z-10">
            <div className="mb-1 text-xs font-semibold text-cyan-400 uppercase tracking-wider">
              Step {currentStep + 1} of {steps.length}
            </div>
            
            <AnimatePresence mode="wait">
              <motion.div
                key={currentStep}
                initial={{ opacity: 0, x: 10 }}
                animate={{ opacity: 1, x: 0 }}
                exit={{ opacity: 0, x: -10 }}
                transition={{ duration: 0.2 }}
              >
                <h3 className="text-lg font-bold mb-2 text-white drop-shadow-md">
                  {step.title}
                </h3>
                <p className="text-sm text-slate-300 leading-relaxed">
                  {step.content}
                </p>
              </motion.div>
            </AnimatePresence>
          </div>

          {/* Footer / Controls */}
          <div className="px-4 py-3 bg-black/20 border-t border-white/5 flex items-center justify-between">
            <div className="flex items-center gap-1">
              <Button
                variant="ghost"
                size="icon"
                onClick={() => setIsPlaying(!isPlaying)}
                className="h-8 w-8 hover:bg-white/10 hover:text-cyan-400 text-slate-400 transition-colors"
              >
                {isPlaying ? <Pause className="w-4 h-4" /> : <Play className="w-4 h-4" />}
              </Button>
            </div>

            <div className="flex items-center gap-2">
              <Button
                variant="ghost"
                size="sm"
                onClick={handlePrev}
                disabled={currentStep === 0}
                className="h-8 px-2 text-slate-400 hover:text-white hover:bg-white/10 disabled:opacity-30"
              >
                <ChevronLeft className="w-4 h-4 mr-1" />
                Back
              </Button>
              
              {currentStep === steps.length - 1 ? (
                <Button 
                  size="sm" 
                  onClick={onClose}
                  className="h-8 bg-cyan-500 hover:bg-cyan-400 text-black font-semibold shadow-[0_0_15px_rgba(6,182,212,0.4)] border-none"
                >
                  Finish
                </Button>
              ) : (
                <Button
                  size="sm"
                  onClick={handleNext}
                  className="h-8 bg-white/10 hover:bg-white/20 text-white border border-white/5"
                >
                  Next
                  <ChevronRight className="w-4 h-4 ml-1" />
                </Button>
              )}
            </div>
          </div>
          
          {/* Progress Bar */}
          <div className="absolute bottom-0 left-0 h-1 bg-white/5 w-full">
            <motion.div 
              className="h-full bg-cyan-400 shadow-[0_0_10px_rgba(34,211,238,0.8)]"
              initial={{ width: "0%" }}
              animate={{ width: `${((currentStep + 1) / steps.length) * 100}%` }}
              transition={{ type: "spring", stiffness: 100, damping: 20 }}
            />
          </div>
        </motion.div>
      )}
    </AnimatePresence>
  );
}
```

## Usage Example

Use the component in your layout or page as follows:

```tsx
import { useState } from 'react';
import { GuidedTourDialog } from './components/guided-tour-dialog';

export default function App() {
  const [showTour, setShowTour] = useState(true);

  const steps = [
    {
      title: "Welcome",
      content: "This is a guided tour of the new features."
    },
    {
      title: "Analytics",
      content: "View your real-time data here."
    }
  ];

  return (
    <div>
      {/* Your App Content */}
      
      <GuidedTourDialog 
        isOpen={showTour} 
        onClose={() => setShowTour(false)} 
        steps={steps} 
      />
    </div>
  );
}
```
