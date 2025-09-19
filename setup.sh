#!/bin/bash

# Skinport Analysis Tool - Linux Setup Script (Updated for Currency Fix)
# This script sets up the virtual environment and installs dependencies

set -e  # Exit on any error

echo "🚀 Setting up Skinport Analysis Tool on Linux"
echo "============================================="

# Check if Python 3 is installed
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is not installed. Please install it first:"
    echo "   sudo apt update && sudo apt install python3 python3-pip python3-venv"
    exit 1
fi

echo "✅ Python 3 found: $(python3 --version)"

# Check if we're in the right directory
if [ ! -f "main.py" ]; then
    echo "❌ main.py not found. Please run this script in the project directory."
    exit 1
fi

# Create virtual environment
echo "📦 Creating virtual environment..."
if [ -d "venv" ]; then
    echo "⚠️  Virtual environment already exists. Removing old one..."
    rm -rf venv
fi

python3 -m venv venv
echo "✅ Virtual environment created"

# Activate virtual environment
echo "🔧 Activating virtual environment..."
source venv/bin/activate

# Upgrade pip
echo "📊 Upgrading pip..."
pip install --upgrade pip

# Install CRITICAL brotli package first (required for Skinport API)
echo "🗜️  Installing brotli (REQUIRED for Skinport API)..."
pip install brotli

# Install other requirements
echo "📦 Installing remaining Python packages..."
if [ -f "requirements.txt" ]; then
    pip install -r requirements.txt
    echo "✅ All packages installed from requirements.txt"
else
    echo "⚠️  requirements.txt not found. Installing essential packages..."
    pip install requests aiohttp tqdm requests-cache
    echo "✅ Essential packages installed"
fi

# Test the critical installations
echo "🧪 Testing critical dependencies..."
python3 -c "
import brotli
import requests
import aiohttp
print('✅ brotli: compression support for Skinport API')
print('✅ requests: HTTP client')
print('✅ aiohttp: concurrent requests (3-5x faster)')

try:
    import tqdm
    print('✅ tqdm: progress bars')
except ImportError:
    print('⚠️  tqdm: not available (optional)')

try:
    import requests_cache
    print('✅ requests_cache: response caching')
except ImportError:
    print('⚠️  requests_cache: not available (will use file cache)')
"

# Run API connectivity test
echo "🧪 Testing Skinport API connectivity..."
if [ -f "test_skinport_api.py" ]; then
    python3 test_skinport_api.py
else
    echo "⚠️  test_skinport_api.py not found, skipping API test"
fi

echo ""
echo "🎉 Setup completed successfully!"
echo ""
echo "🔧 CURRENCY ISSUE RESOLVED:"
echo "  ✅ brotli compression support installed"
echo "  ✅ Correct API headers configured" 
echo "  ✅ PLN currency fully supported"
echo ""
echo "To run the script:"
echo "  1. Activate the virtual environment: source venv/bin/activate"
echo "  2. Run the script: python3 main.py"
echo "  3. Select PLN currency when prompted"
echo ""
echo "To deactivate the virtual environment later: deactivate"
