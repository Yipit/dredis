## Steps to release a new version

1. Update `dredis/__init__.py` with the new version
1. Update `CHANGELOG.md` section "Not released yet" to have new features, bugfixes, backward incompatible changes, etc. 
1. Commit & push the changelog updates
1. Run `make release` and enter the new version. This command will update the changelog, commit, create a git tag, and upload to PyPI
