rmdir build
mkdir build
rmdir wheel
mkdir wheel
python.exe setup.py bdist_wheel -d build
copy build\*.whl wheel