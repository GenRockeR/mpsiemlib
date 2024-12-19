pip install sphinx
pip install sphinx-rtd-theme

cd sphinx
call .\make.bat clean

rm ../docs/*
rm ./mpsiemlib*.rst
rm ./modules.rst

sphinx-apidoc -e -M -o ./ ../mpsiemlib/
call .\make.bat html

cp ./_build/html/* ../docs/
