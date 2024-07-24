@ECHO OFF

pushd %~dp0
set script_dir=%CD%
popd

cd %script_dir%

python --version >nul 2>&1 && set python=python || (
   python3 --version >nul 2>&1 && set python=python3 || (
      echo Python não encontrado, por favor instale o Python 3.x para prosseguir!
      PAUSE
      goto :EOF
   )
)

if exist start_app.exe (
   echo executável já existe, caso deseje reinstalar, por favor delete o executável e tente novamente!
   PAUSE
   goto :EOF
)

virtualenv --version >nul 2>&1 || (
   echo virtualenv não instalado, instalando...
   %python% -m pip install virtualenv   
   echo virtualenv instalado com êxito!
)

echo Criando ambiente virtual...

if exist .venv\ (
   echo Ambiente virtual já existe, prosseguindo...
   call .venv\Scripts\activate.bat
) else (
   virtualenv .venv
   call .venv\Scripts\activate.bat
   pip install -U pip

   if exist requirements.txt (
      pip install -r requirements.txt
   )

   if exist setup.py (
      pip install -e .
   )
)

pyinstaller --version >nul 2>&1 || (
   echo pyinstaller não instalado, instalando...
   %python% -m pip install pyinstaller
   echo pyinstaller instalado com êxito!
)

echo instalando app...
pyinstaller -w --onefile --additional-hooks-dir=. start_app.py
move .\dist\start_app.exe .
del /q start_app.spec
del /s /q dist\*
del /s /q build\*
echo Instalação concluída, utilize o executável criádo para iniciar o app!

PAUSE
