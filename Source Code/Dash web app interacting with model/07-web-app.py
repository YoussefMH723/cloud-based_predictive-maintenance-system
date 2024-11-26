EXPLAINER_PATH = "models/explainer.pickle.dat"
MODEL_PATH = "models/xgboost.pickle.dat"
COLUMNS = ['Air temperature K', 'Process temperature K',
           'Rotational speed rpm', 'Torque Nm', 'Tool wear min', 'Air times process temp',
           'Rot spd ovr torque', 'Rot spd times tool wr', 'Torque times tool wr', 'Type_M',
            'Type_L', 'Type_H']
FAILURE = ["No Failure", "Failure"]

import dash
import shap
import pandas as pd
import matplotlib.pyplot as plt
from dash import html, dcc, Input, Output, State

import pickle
import base64
from io import BytesIO

explainer = pickle.load(open(EXPLAINER_PATH, "rb"))
model = pickle.load(open(MODEL_PATH, "rb"))

empty_data = {i:[0] for i in COLUMNS}

app = dash.Dash(__name__)

app.layout = html.Div([
    html.H1(["Machine Failure Classification"]),
    html.Div([
        html.Div([
            html.H3("Enter Parameters:", className="h3"),
            html.Div([
                html.Div([
                    html.Label("Air temperature (K): "),
                    html.Br(),
                    html.Label("Process temperature (K): "),
                    html.Br(),
                    html.Label("Rotational speed (rpm): "),
                    html.Br(),
                    html.Label("Torque (Nm): "),
                    html.Br(),
                    html.Label("Tool wear (min): "),
                    html.Br(),
                    html.Label("Type: ")
                ], className="LabelDiv"),
                html.Div([
                    dcc.Input(id='air_temp', type='number', placeholder='Air temperature (K)', value=298, className='inputbox'),
                    html.Br(),
                    dcc.Input(id='process_temp', type='number', placeholder='Process temperature (K)', value=304, className='inputbox'),
                    html.Br(), 
                    dcc.Input(id='rot_spd', type='number', placeholder='Rotational speed (rpm)', value=1550, className='inputbox'),
                    html.Br(),
                    dcc.Input(id='trq', type='number', placeholder='Torque (Nm)', value=50, className='inputbox'),
                    html.Br(),
                    dcc.Input(id='tool_wear', type='number', placeholder='Tool wear (min)', value=8, className='inputbox'),
                    html.Br(),
                    dcc.RadioItems(
                        ['Type_M', 'Type_L', 'Type_H'],
                        "Type_M",
                        inline=True,
                        id='typ'
                    ),
                ], className="InputDiv"),
            ], className="ParameterDiv"),
            html.Div([
                html.Button('Submit', id="my-button")
            ], className="ButtonDiv"),
            html.Div([
                html.H2(id="prediction"),
                html.H2(id="prediction_proba"),
            ]),
        ], className='InnerDiv'),
        html.Div([
            html.Img(id='graph', height=500, width=700)
        ], className="ImageDiv"),
    ], className="ContentDiv")
], className='OuterDiv')


@app.callback(
        Output('graph', 'src'),
        Output('prediction', 'children'),
        Output('prediction_proba', 'children'),
        Input('my-button', 'n_clicks'),
        [
            State('air_temp', 'value'),
            State('process_temp', 'value'),
            State('rot_spd', 'value'),
            State('trq', 'value'),
            State('tool_wear', 'value'),
            State('typ', 'value'),
        ]
        )
def shap_waterfall(n_clicks, air_temp, process_temp, rot_spd, trq, tool_wear, typ):
    data = empty_data.copy()
    data['Air temperature K'] = [air_temp]; data['Process temperature K'] = [process_temp]
    data['Rotational speed rpm'] = [rot_spd]; data['Torque Nm'] = [trq]
    data['Tool wear min'] = [tool_wear]; data['Air times process temp'] = [air_temp*process_temp]
    data['Rot spd ovr torque'] = rot_spd/trq; data['Rot spd times tool wr'] = rot_spd*tool_wear
    data['Torque times tool wr'] = [trq*tool_wear]; data[typ] = [1]

    data = pd.DataFrame(data)

    shap_value = explainer(data)
    fig = plt.figure()
    shap.plots.waterfall(shap_value[0], show=False)
    plt.tight_layout()
    buf = BytesIO()
    fig.savefig(buf, format="png")
    # Embed the result in the html output.
    fig_data = base64.b64encode(buf.getbuffer()).decode("ascii")
    fig_matplotlib = f'data:image/png;base64,{fig_data}'

    prediction = f"Model Prediction is: {FAILURE[int(model.predict(data)[0])]}"
    prediction_proba = f"Machine Failure Probability is: {model.predict_proba(data)[0][1]*100:.2f}%"
    
    return fig_matplotlib, prediction, prediction_proba


if __name__ == "__main__":
    app.run_server(debug=True)