#!/bin/bash

create_venv() {
   if ! command -v virtualenv &> /dev/null; then
      echo "virtualenv is not installed. Installing..."
      $python -m pip install --user virtualenv --break-system-packages
      echo "virtualenv installation complete."
   fi

   local env_name=${1:-".venv"}

   if [ -d "$env_name" ]; then
      echo "Virtual environment '$env_name' already exists. Aborting."
      return 1
   fi

   virtualenv "$env_name"
   source "./$env_name/bin/activate"
   pip install -U pip

   if [ -f "requirements.txt" ]; then
      pip install -r ./requirements.txt
   fi

   if [ -f "setup.py" ]; then
      pip install -e .
   fi
}

install_app() {

   if ! command -v pyinstaller &> /dev/null; then
      echo "pyinstaller is not installed. Installing..."
      $python -m pip install --user pyinstaller --break-system-packages
      echo "pyinstaller installation complete."
   fi

   pyinstaller -w --onefile --additional-hooks-dir=. start_app.py
   mv ./dist/start_app .
   rm -rf ./start_app.spec
   rm -rf ./dist
   rm -rf ./build
   echo "Instalação concluída, utilize o executável criado para iniciar o app!"
}

if command -v python &> /dev/null;
   then
      python=python
   elif command -v python3 &> /dev/null;
      then
         python=python3
      else
         echo "Python não instalado, por favor instale o Python 3.x para prosseguir!"
         read -p "Press any button to continue..."
         exit
fi

if ! command -v pip &> /dev/null;
   then
      echo "pip não instalado, tentando instalar o pip..."
      $python -m ensurepip --upgrade
      if ! command -v pip /dev/null:
         then
            echo "Falha na instalação do pip, por favor instale o pip e tente novamente!"
            read -p "Press any button to continue..."
            exit
      fi
fi

create_venv
install_app
