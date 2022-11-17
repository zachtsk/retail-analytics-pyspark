# Install package
python setup.py -q bdist_wheel &&
# Grab the latest version
fn=$(ls -t ./dist/*.whl | head -n1) &&
# Install package
pip install --upgrade --no-deps --force-reinstall "./$fn" &&
echo "!!! Package Re-build complete"