# Minimal makefile for Sphinx documentation
#

# You can set these variables from the command line.
SPHINXOPTS    =
SPHINXBUILD   = python -msphinx
SPHINXPROJ    = stepped_job
SOURCEDIR     = source
BUILDDIR      = ../../stepped_job-docs
GITCOMMANDS   = cd $(BUILDDIR)/html; git add . ; git commit -m 'Rebuilt docs' ; git push origin gh-pages

# Put it first so that "make" without argument is like "make help".
help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)
	@echo 'If the target is "gh-pages", "html" will be built, and the '\
	'result will be automatically published through the command'
	@echo "$(GITCOMMANDS)"

.PHONY: help Makefile

# Target to build the GitHub Pages
gh-pages: html
	$(GITCOMMANDS)

# Target to update the notebooks
notebooks: source/notebooks/*.ipynb
	jupyter nbconvert --execute --inplace $^

# Necessary so "make" does not try to build the ".ipynb" files (by calling Sphinx)
%.ipynb:

# Catch-all target: route all unknown targets to Sphinx using the new
# "make mode" option.  $(O) is meant as a shortcut for $(SPHINXOPTS).
%: Makefile
	@if [ -d $(BUILDDIR)/doctrees ]; then rm -r $(BUILDDIR)/doctrees; fi
	@if [ -d source/api ]; then rm -r source/api; fi
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)
