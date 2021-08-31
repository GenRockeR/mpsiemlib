rem pip install sphinx
rem pip install sphinx-rtd-theme
rem sphinx-quickstart
cd .\sphinx
call .\make.bat clean

rm ..\docs\*
rm .\mpsiemlib*.rst
rm .\modules.rst

sphinx-apidoc -e -M -o .\ ..\mpsiemlib\
call .\make.bat html

xcopy .\_build\html\* ..\docs\ /s /e
