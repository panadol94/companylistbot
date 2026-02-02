@echo off
echo ===================================================
echo   AUTO GITHUB UPLOADER
echo ===================================================
echo.
echo Repo URL: https://github.com/panadol94/companylistbot.git
echo.

echo Setting remote...
git remote remove origin
git remote add origin https://github.com/panadol94/companylistbot.git

echo.
echo Menukar nama branch ke 'main'...
git branch -M main

echo.
echo Sedang upload ke GitHub... 
echo (Sila login di browser jika popup keluar)
git push -u origin main

echo.
echo ===================================================
echo   SIAP! Sila semak GitHub anda:
echo   https://github.com/panadol94/companylistbot
echo ===================================================
pause
