"""
CLI entry point for mentat-gui package.
"""

import asyncio
import argparse
import logging
from datetime import datetime, timedelta
from pathlib import Path

from config import get_settings
from signal_scanner import SignalScanner

logger = logging.getLogger(__name__)

def setup_logging(verbose: bool = False) -> None:
    """Setup logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s %(levelname)s %(message)s',
        handlers=[
            logging.StreamHandler()
        ]
    )

def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Crypto Signal Scanner CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  mentat-scan --days 365 --top 5
  mentat-scan --start 2023-01-01 --end 2024-01-01 --series "BTCUSDT,Fear-and-Greed"
  mentat-scan --no-plots --verbose
        """
    )
    
    # Date range options
    parser.add_argument(
        '--days', 
        type=int, 
        default=None,
        help='Number of days to look back (default: from config)'
    )
    parser.add_argument(
        '--start', 
        type=str, 
        default=None,
        help='Start date (YYYY-MM-DD format)'
    )
    parser.add_argument(
        '--end', 
        type=str, 
        default=None,
        help='End date (YYYY-MM-DD format, default: today)'
    )
    
    # Analysis options
    parser.add_argument(
        '--max-lag', 
        type=int, 
        default=None,
        help='Maximum lag to test (default: from config)'
    )
    parser.add_argument(
        '--top', 
        type=int, 
        default=None,
        help='Number of top correlations to return (default: from config)'
    )
    parser.add_argument(
        '--series', 
        type=str, 
        default=None,
        help='Comma-separated list of series to analyze (default: all)'
    )
    
    # Output options
    parser.add_argument(
        '--no-plots', 
        action='store_true',
        help='Skip generating plots'
    )
    parser.add_argument(
        '--output-dir', 
        type=str, 
        default=None,
        help='Output directory for results (default: current directory)'
    )
    
    # Other options
    parser.add_argument(
        '--verbose', '-v', 
        action='store_true',
        help='Enable verbose logging'
    )
    parser.add_argument(
        '--no-numba', 
        action='store_true',
        help='Disable Numba acceleration'
    )
    
    return parser.parse_args()

def parse_date(date_str: str) -> datetime:
    """Parse date string in YYYY-MM-DD format."""
    try:
        return datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date format: {date_str}. Use YYYY-MM-DD")

async def main() -> None:
    """Main CLI function."""
    args = parse_args()
    setup_logging(args.verbose)
    
    settings = get_settings()
    
    # Parse dates
    end = datetime.now()
    if args.end:
        end = parse_date(args.end)
    
    if args.start:
        start = parse_date(args.start)
    elif args.days:
        start = end - timedelta(days=args.days)
    else:
        start = end - timedelta(days=settings.lookback_days)
    
    # Parse series list
    series_names = None
    if args.series:
        series_names = [s.strip() for s in args.series.split(',')]
    
    # Setup output directory
    output_dir = args.output_dir or Path.cwd()
    if isinstance(output_dir, str):
        output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True)
    
    logger.info(f"Starting signal scan from {start.date()} to {end.date()}")
    
    try:
        # Create scanner
        scanner = SignalScanner(use_numba=not args.no_numba)
        
        # Run scan
        results = await scanner.scan_signals(
            start=start,
            end=end,
            series_names=series_names,
            max_lag=args.max_lag,
            top_n=args.top
        )
        
        if 'error' in results:
            logger.error(f"Scan failed: {results['error']}")
            return
        
        # Save results
        scanner.save_results(results, output_dir)
        
        # Print summary
        print(f"\nScan Results:")
        print(f"  Period: {start.date()} to {end.date()}")
        print(f"  Series analyzed: {results['series_count']}")
        print(f"  Data points: {results['data_points']}")
        print(f"  Max lag tested: {results['max_lag']}")
        
        if not results['top_correlations'].empty:
            print(f"\nTop correlations:")
            for _, row in results['top_correlations'].iterrows():
                print(f"  {row['lead_series']} → {row['lag_series']} "
                      f"(lag: {row['lag']:2d}, corr: {row['correlation']:.3f})")
        
        # Generate plots if requested
        if not args.no_plots:
            await generate_plots(results, output_dir)
        
        print(f"\nResults saved to: {output_dir}")
        
    except Exception as e:
        logger.error(f"Scan failed: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()

async def generate_plots(results: dict, output_dir: Path) -> None:
    """Generate plots from scan results."""
    try:
        import matplotlib.pyplot as plt
        
        # Correlation heatmap
        if not results['all_correlations'].empty:
            plt.figure(figsize=(12, 8))
            
            # Create pivot table for heatmap
            pivot_data = results['all_correlations'].pivot_table(
                index='lead_series', 
                columns='lag_series', 
                values='correlation',
                aggfunc='mean'
            )
            
            plt.imshow(pivot_data, cmap='RdBu_r', aspect='auto')
            plt.colorbar(label='Correlation')
            plt.xticks(range(len(pivot_data.columns)), pivot_data.columns, rotation=45)
            plt.yticks(range(len(pivot_data.index)), pivot_data.index)
            plt.title('Lead-Lag Correlation Matrix')
            plt.tight_layout()
            plt.savefig(output_dir / 'correlations.png', dpi=300, bbox_inches='tight')
            plt.close()
        
        # Composite signal
        if not results['composite_signal'].empty:
            plt.figure(figsize=(12, 6))
            results['composite_signal'].plot()
            plt.title('Composite Signal')
            plt.xlabel('Date')
            plt.ylabel('Z-Score')
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            plt.savefig(output_dir / 'composite_signal.png', dpi=300, bbox_inches='tight')
            plt.close()
            
    except ImportError:
        logger.warning("matplotlib not available - skipping plots")
    except Exception as e:
        logger.warning(f"Failed to generate plots: {e}")

if __name__ == '__main__':
    asyncio.run(main()) 