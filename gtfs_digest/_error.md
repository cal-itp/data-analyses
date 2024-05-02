Running Jupyter-Book v1.0.0
Source Folder: /home/jovyan/data-analyses/portfolio/gtfs_digest
Config Path: /home/jovyan/data-analyses/portfolio/gtfs_digest/_config.yml
Output Path: /home/jovyan/data-analyses/portfolio/gtfs_digest/_build/html
Running Sphinx v5.3.0
making output directory... done
[etoc] Changing master_doc to 'README'
myst v1.0.0: MdParserConfig(commonmark_only=False, gfm_only=False, enable_extensions={'tasklist', 'dollarmath', 'substitution', 'colon_fence', 'linkify'}, disable_syntax=[], all_links_external=False, url_schemes=('mailto', 'http', 'https'), ref_domains=None, fence_as_directive=set(), number_code_blocks=[], title_to_header=False, heading_anchors=0, heading_slug_func=None, html_meta={}, footnote_transition=True, words_per_minute=200, substitutions={}, linkify_fuzzy_links=True, dmath_allow_labels=True, dmath_allow_space=True, dmath_allow_digits=True, dmath_double_inline=False, update_mathjax=True, mathjax_classes='tex2jax_process|mathjax_process|math|output_area', enable_checkboxes=False, suppress_warnings=[], highlight_code_blocks=True)
myst-nb v1.0.0: NbParserConfig(custom_formats={}, metadata_key='mystnb', cell_metadata_key='mystnb', kernel_rgx_aliases={}, eval_name_regex='^[a-zA-Z_][a-zA-Z0-9_]*$', execution_mode='off', execution_cache_path='', execution_excludepatterns=[], execution_timeout=-1, execution_in_temp=False, execution_allow_errors=False, execution_raise_on_error=False, execution_show_tb=False, merge_streams=False, render_plugin='default', remove_code_source=False, remove_code_outputs=False, code_prompt_show='Show code cell {type}', code_prompt_hide='Hide code cell {type}', number_source_lines=False, output_stderr='show', render_text_lexer='myst-ansi', render_error_lexer='ipythontb', render_image_options={}, render_figure_options={}, render_markdown_format='commonmark', output_folder='build', append_css=True, metadata_to_fm=False)
Using jupyter-cache at: /home/jovyan/data-analyses/portfolio/gtfs_digest/_build/.jupyter_cache
sphinx-multitoc-numbering v0.1.3: Loaded
The default value for `navigation_with_keys` will change to `False` in the next release. If you wish to preserve the old behavior for your site, set `navigation_with_keys=True` in the `html_theme_options` dict in your `conf.py` file. Be aware that `navigation_with_keys = True` has negative accessibility implications: https://github.com/pydata/pydata-sphinx-theme/issues/1492
building [mo]: targets for 0 po files that are out of date
building [html]: targets for 88 source files that are out of date
updating environment: [new config] 88 added, 0 changed, 0 removed
reading sources... [100%] district_12-irvine/0__03_report__district_12-irvine__organization_name_orange-county-transportation-authority                                                                     

Sphinx error:
root file /home/jovyan/data-analyses/portfolio/gtfs_digest/README.rst not found
Traceback (most recent call last):
  File "/opt/conda/lib/python3.9/site-packages/jupyter_book/sphinx.py", line 167, in build_sphinx
    app.build(force_all, filenames)
  File "/opt/conda/lib/python3.9/site-packages/sphinx/application.py", line 347, in build
    self.builder.build_update()
  File "/opt/conda/lib/python3.9/site-packages/sphinx/builders/__init__.py", line 310, in build_update
    self.build(to_build,
  File "/opt/conda/lib/python3.9/site-packages/sphinx/builders/__init__.py", line 326, in build
    updated_docnames = set(self.read())
  File "/opt/conda/lib/python3.9/site-packages/sphinx/builders/__init__.py", line 436, in read
    raise SphinxError('root file %s not found' %
sphinx.errors.SphinxError: root file /home/jovyan/data-analyses/portfolio/gtfs_digest/README.rst not found

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "/opt/conda/bin/jb", line 8, in <module>
    sys.exit(main())
  File "/opt/conda/lib/python3.9/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/opt/conda/lib/python3.9/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/opt/conda/lib/python3.9/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/opt/conda/lib/python3.9/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/opt/conda/lib/python3.9/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/opt/conda/lib/python3.9/site-packages/jupyter_book/cli/main.py", line 317, in build
    builder_specific_actions(
  File "/opt/conda/lib/python3.9/site-packages/jupyter_book/cli/main.py", line 528, in builder_specific_actions
    raise RuntimeError(_message_box(msg, color="red", doprint=False)) from result
RuntimeError: 
===============================================================================

There was an error in building your book. Look above for the cause.

===============================================================================

Traceback (most recent call last):

  File "/home/jovyan/data-analyses/portfolio/portfolio.py", line 437, in <module>
    app()

  File "/home/jovyan/data-analyses/portfolio/portfolio.py", line 404, in build
    subprocess.run(

  File "/opt/conda/lib/python3.9/subprocess.py", line 460, in check_returncode
    raise CalledProcessError(self.returncode, self.args, self.stdout,

subprocess.CalledProcessError: Command '['jb', 'build', '-W', '-n', '--keep-going', '.']' returned non-zero exit status 1.

jovyan@jupyter-amandaha8 ~/data-analyses (ah_gtfs_port) $ 