
"""
Creado por: Yohan Alfonso Hernandez
Fecha 05-07-2023
Tema: dag en python para ETL en dataflow
"""

start_python_job_async = BeamRunPythonPipelineOperator(
    task_id="start_python_job_async",
    runner=BeamRunnerType.DataflowRunner,
    py_file=GCS_PYTHON_SCRIPT,
    py_options=[],
    pipeline_options={
        "output": GCS_OUTPUT,
    },
    py_requirements=["apache-beam[gcp]==2.46.0"],
    py_interpreter="python3",
    py_system_site_packages=False,
    dataflow_config={
        "job_name": "start_python_job_async",
        "location": LOCATION,
        "wait_until_finished": False,
    },
)