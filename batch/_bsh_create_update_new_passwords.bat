@ECHO OFF

REM Definir el nombre de los scripts a ejecutar
SET SCRIPT_NAME_EXECUTE=_etl_gateway_aib_bi.py

REM Definir las rutas relativas de los scripts y el entorno virtual
SET PATH_SCRIPTS=.\src
SET PATH_VENV=.\..

REM Cambiar al directorio del entorno virtual
cd %PATH_VENV%

REM Activar el entorno virtual en Windows
call venv\Scripts\activate.bat

REM Verificar si el entorno virtual está activado
IF NOT DEFINED VIRTUAL_ENV (
    ECHO El entorno virtual NO está activado.
    PAUSE
    EXIT /B
) ELSE (
    ECHO El entorno virtual está activado: %VIRTUAL_ENV%
)

REM Ejecutar los scripts de Python en el entorno virtual
ECHO Ejecutando el Script %SCRIPT_NAME_EXECUTE%
python %PATH_SCRIPTS%\%SCRIPT_NAME_EXECUTE%

TIMEOUT 5

EXIT