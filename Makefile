VENV = .venv/bin
APP = task_schedule.py
PY = python3

run: ./$(VENV)/activate
	./$(VENV)/$(PY) $(APP)

$(VENV)/activate: requirements.txt
	$(PY) -m venv .venv
	./$(VENV)/pip install -r requirements.txt

clean:
	rm -rf __pycache__
	rm -rf .venv
