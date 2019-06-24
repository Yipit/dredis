read -p "What's the next version? " new_version

sed -i.bkp "s/__version__ = .*/__version__ = '$new_version'/" dredis/__init__.py
awk "NR == 2 {print \"\n## $new_version\"} {print \$0}" CHANGELOG.md > NEW-CHANGELOG.md
mv NEW-CHANGELOG.md CHANGELOG.md

git add dredis/__init__.py CHANGELOG.md && \
    git commit -m "Release $new_version" && \
    git push origin master && \
    git tag $new_version && \
    git push --tags

python setup.py sdist bdist_wheel && \
    twine upload dist/*
