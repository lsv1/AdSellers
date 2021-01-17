import flask
from flask import render_template

app = flask.Flask('my app')

if __name__ == "__main__":
    file_name = 'adstxt_pie.html'
    with app.app_context():
        rendered = render_template(file_name,
                                   page_title="Nile Delta - Overall share of SSPs",
                                   )

        with open('output_html/' + file_name, 'w') as f:
            f.write(str(rendered))
