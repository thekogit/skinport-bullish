#!/usr/bin/env python3
import subprocess
import sys
import os

def install_dependencies():
    print("Installing dependencies...")

    try:
        # Install core dependencies
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', '-r', 'requirements.txt'])
        print("✅ Dependencies installed successfully!")

        # Test imports
        try:
            import aiohttp
            print("⚡ aiohttp available - concurrent fetching enabled!")
        except ImportError:
            print("⚠️  aiohttp not available - install for better performance: pip install aiohttp")

        try:
            import tqdm
            print("📊 tqdm available - progress bars enabled!")
        except ImportError:
            print("⚠️  tqdm not available - install for progress bars: pip install tqdm")

    except subprocess.CalledProcessError as e:
        print(f"❌ Error installing dependencies: {e}")
        return False

    return True

def main():
    print("🚀 Skinport Analysis Tool - Setup")
    print("=" * 40)

    if not os.path.exists('requirements.txt'):
        print("❌ requirements.txt not found. Make sure you're in the correct directory.")
        return

    if install_dependencies():
        print("\n🎉 Setup complete! You can now run:")
        print("   python main.py")
    else:
        print("\n❌ Setup failed. Please check the error messages above.")

if __name__ == "__main__":
    main()
