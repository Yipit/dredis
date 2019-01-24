## Steps to release a new version

1. Update `dredis/__init__.py` with the new version
1. Update `CHANGELOG.md` to have new features, bugfixes, backward incompatible changes, etc 
1. Commit the previous changes using the following format:
    ```text
    Release 1.0.0
    
    * changelog entry 1
    * changelog entry 2
    ...
    ```
1. Create a git tag, push it to Github, upload new version to PyPI (`git tag NEW_VERSION && git push --tags && make release`)
