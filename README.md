# ⚙️ Section 1A - DAG folder, .airflowignore, airflow.cfg

> Where Airflow looks for DAGs, what it ignores,
> and how to keep scripts from running by accident.

---

## Where Airflow looks for DAGs
In `airflow/airflow.cfg` I changed `dags_folder` so it points at the whole
`data_course` folder instead of just `airflow/dags`:

```
dags_folder = /home/dominicbrookes/data_course
```

Airflow searches every subfolder, so any project's DAGs are found
automatically. To undo it, set the line back to `.../data_course/airflow/dags`.

## How Airflow finds DAGs
- Every few minutes Airflow checks every `.py` file in `data_course`,
  whatever it is called. Other file types (`.csv`, `.md`, `.ipynb`) are ignored.
- If a file contains both "airflow" and "dag" (any capitals), Airflow
  runs it to see if a DAG is defined inside.
- If it contains a DAG, the DAG appears in Airflow. If not, nothing happens.

## The `.airflowignore` file
- Lives in `data_course`, next to `.gitignore`.
- Each line is a folder name. Airflow skips that whole folder and never
  reads anything inside it.
- Lines starting with `#` are comments.
- Currently skipped: `.venv_airflow` (Airflow's own install), `.vscode`,
  `01_notebooks`, `logs`.
- New project folders do not need adding. The "airflow and dag" check
  already skips normal scripts.

## Protecting scripts with the main block
If a script in a scanned folder ever contains both words, Airflow will
run it, and any code outside a function will execute. To stop this,
anything that actually runs goes at the bottom inside:

```python
if __name__ == "__main__":
    run_bronze(sys.argv[1])
```

This only runs when I start the file myself in the terminal, not when
Airflow opens it or a DAG imports it.

## Why not a separate Airflow per project?
A new VS Code workspace does not change Airflow. Airflow only reads
`airflow.cfg`. A separate Airflow would mean a new venv, installing
Airflow again, a new home folder, database and login, and stopping the
old one first (both use port 8080). Pointing one Airflow at `data_course`
is a one-time change that works for every project.

I could still set up a separate one in future, for example if a project
needs a different Airflow version or must be completely separate.