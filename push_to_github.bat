@echo off
echo ===================================================
echo   Push to GitHub Helper Script
echo ===================================================
echo.
set /p REPO_URL="Enter your GitHub Repository URL (e.g. https://github.com/your-username/your-repo.git): "

if "%REPO_URL%"=="" (
    echo Error: Repository URL cannot be empty.
    pause
    exit /b
)

echo.
echo 1. Adding remote origin...
git remote remove origin 2>nul
git remote add origin %REPO_URL%

echo 2. Renaming default branch to main...
git branch -M main

echo 3. Pushing code to GitHub...
echo (You may be prompted by GitHub for login credentials or authentication in a browser popup)
git push -u origin main

echo.
echo ===================================================
echo   Deployment completed!
echo ===================================================
pause
