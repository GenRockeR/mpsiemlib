rm -rf build wheel
mkdir -p build wheel
python setup.py bdist_wheel -d build
mv build/*.whl wheel
rm -rf build