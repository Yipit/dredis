## Steps to release a new version

1. Update `CHANGELOG.md` "Not yet released" section to have all important changes since the last release 
1. Run `make release` and enter the new version. This command will update the changelog, commit, create a git tag, and upload to PyPI
