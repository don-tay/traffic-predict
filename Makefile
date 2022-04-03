VENV = .venv/bin
APP = task_schedule.py

run: ./$(VENV)/activate
	./$(VENV)/python3 $(APP)

$(VENV)/activate: requirements.txt
	python3 -m venv .venv
	./$(VENV)/pip install -r requirements.txt

clean:
	rm -rf __pycache__
	rm -rf .venv
