@echo off
echo ============================================================
echo PDF Translation Setup
echo ============================================================
echo.
echo Please enter your OpenAI API key:
set /p OPENAI_API_KEY="API Key: "
echo.
echo Starting translation process...
echo ============================================================
echo.

py "C:\Users\denpo\OneDrive\Coding\Ai Holy Mother Han\pdf_translator_openai.py"

pause
