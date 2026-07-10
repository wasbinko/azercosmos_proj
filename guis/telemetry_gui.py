import sys, os, subprocess, threading, queue

try:
    import tkinter as tk
    from tkinter import ttk, scrolledtext, messagebox
except ImportError:
    print("ERROR: tkinter is not available in this Python installation.")
    sys.exit(1)

MAX_LOG_LINES = 2000


def build_generator_command(python_exe: str, script_path: str, sink: str,
                            kafka_bootstrap: str, kafka_topic: str,
                            anomaly_probability: float, interval: int) -> list[str]:
    """Pure command-building logic, testable without a GUI event loop."""
    cmd = [python_exe, "-u", script_path, "--sink", sink,
           "--anomaly_probability", str(anomaly_probability),
           "--interval", str(interval)]
    if sink in ("kafka", "both"):
        cmd += ["--kafka_bootstrap", kafka_bootstrap, "--kafka_topic", kafka_topic]
    return cmd


class GeneratorGUI:
    def __init__(self, root):
        self.root = root
        root.title("Telemetry Generator Control")
        root.geometry("720x600")
        root.minsize(600, 480)

        self.log_queue = queue.Queue()
        self.process = None
        self.project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self._log_line_count = 0

        self._build_ui()
        self._poll_queue()
        root.protocol("WM_DELETE_WINDOW", self._on_close)

    def _build_ui(self):
        sink_frame = ttk.LabelFrame(self.root, text="Output", padding=10)
        sink_frame.pack(fill="x", padx=10, pady=(10, 5))

        self.sink_var = tk.StringVar(value="kafka")
        for i, val in enumerate(["csv", "kafka", "both"]):
            ttk.Radiobutton(sink_frame, text=val.upper(), variable=self.sink_var,
                           value=val, command=self._toggle_kafka).grid(row=0, column=i, sticky="w", padx=5)

        ttk.Label(sink_frame, text="Kafka bootstrap:").grid(row=1, column=0, sticky="w", pady=(8, 0))
        self.kafka_bootstrap_var = tk.StringVar(value="localhost:9092")
        self.kafka_bootstrap_entry = ttk.Entry(sink_frame, textvariable=self.kafka_bootstrap_var,
                                               width=30, state="enabled")
        self.kafka_bootstrap_entry.grid(row=1, column=1, columnspan=2, sticky="w", pady=(8, 0))

        ttk.Label(sink_frame, text="Kafka topic:").grid(row=2, column=0, sticky="w", pady=(4, 0))
        self.kafka_topic_var = tk.StringVar(value="telemetry.raw")
        self.kafka_topic_entry = ttk.Entry(sink_frame, textvariable=self.kafka_topic_var,
                                          width=30, state="enabled")
        self.kafka_topic_entry.grid(row=2, column=1, columnspan=2, sticky="w", pady=(4, 0))

        opts_frame = ttk.LabelFrame(self.root, text="Options", padding=10)
        opts_frame.pack(fill="x", padx=10, pady=5)

        ttk.Label(opts_frame, text="Anomaly probability (0.0 - 1.0):").grid(row=0, column=0, sticky="w")
        self.anomaly_var = tk.DoubleVar(value=0.25)
        ttk.Spinbox(opts_frame, from_=0.0, to=1.0, increment=0.05,
                   textvariable=self.anomaly_var, width=10).grid(row=0, column=1, sticky="w")

        ttk.Label(opts_frame, text="Interval in seconds (300 = 5 min; try 5 for fast testing):").grid(
            row=1, column=0, sticky="w", pady=(8, 0))
        self.interval_var = tk.IntVar(value=300)
        ttk.Spinbox(opts_frame, from_=1, to=3600, increment=5,
                   textvariable=self.interval_var, width=10).grid(row=1, column=1, sticky="w", pady=(8, 0))

        button_frame = ttk.Frame(self.root)
        button_frame.pack(pady=8)
        self.start_button = ttk.Button(button_frame, text="Start Generating", command=self._start_generator)
        self.start_button.grid(row=0, column=0, padx=5)
        self.stop_button = ttk.Button(button_frame, text="Stop", command=self._stop_generator, state="disabled")
        self.stop_button.grid(row=0, column=1, padx=5)

        self.status_var = tk.StringVar(value="\u25cf Stopped")
        self.status_label = ttk.Label(self.root, textvariable=self.status_var,
                                      font=("Segoe UI", 11, "bold"), foreground="#888888")
        self.status_label.pack()

        log_frame = ttk.LabelFrame(self.root, text="Generator Log", padding=5)
        log_frame.pack(fill="both", expand=True, padx=10, pady=10)
        self.log_text = scrolledtext.ScrolledText(
            log_frame, height=16, state="disabled", font=("Consolas", 9), wrap="word")
        self.log_text.pack(fill="both", expand=True)

    def _toggle_kafka(self):
        state = "normal" if self.sink_var.get() in ("kafka", "both") else "disabled"
        self.kafka_bootstrap_entry.config(state=state)
        self.kafka_topic_entry.config(state=state)

    def _start_generator(self):
        cmd = build_generator_command(
            python_exe=sys.executable,
            script_path=os.path.join(self.project_root, "generate_telemetry.py"),
            sink=self.sink_var.get(),
            kafka_bootstrap=self.kafka_bootstrap_var.get(),
            kafka_topic=self.kafka_topic_var.get(),
            anomaly_probability=self.anomaly_var.get(),
            interval=self.interval_var.get(),
        )
        self.start_button.config(state="disabled")
        self.stop_button.config(state="normal")
        self.status_var.set("\u25cf Running")
        self.status_label.config(foreground="#1a7a3c")
        self.log_text.config(state="normal")
        self.log_text.delete("1.0", tk.END)
        self.log_text.insert(tk.END, f"Running: {' '.join(cmd)}\n\n")
        self.log_text.config(state="disabled")
        self._log_line_count = 0

        threading.Thread(target=self._run_generator, args=(cmd,), daemon=True).start()

    def _run_generator(self, cmd):
        try:
            child_env = os.environ.copy()
            child_env["PYTHONIOENCODING"] = "utf-8"
            self.process = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, bufsize=1, cwd=self.project_root,
                encoding="utf-8", errors="replace", env=child_env,
            )
            for line in self.process.stdout:
                self.log_queue.put(("line", line))
            self.process.wait()
            self.log_queue.put(("stopped", self.process.returncode))
        except Exception as e:
            self.log_queue.put(("error", str(e)))

    def _stop_generator(self):
        self.stop_button.config(state="disabled")
        if self.process and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()

    def _on_close(self):
        if self.process and self.process.poll() is None:
            if messagebox.askyesno("Generator still running",
                                   "The telemetry generator is still running. Stop it and quit?"):
                self._stop_generator()
                self.root.destroy()
        else:
            self.root.destroy()

    def _poll_queue(self):
        try:
            while True:
                kind, payload = self.log_queue.get_nowait()
                if kind == "line":
                    self.log_text.config(state="normal")
                    self.log_text.insert(tk.END, payload)
                    self._log_line_count += 1
                    if self._log_line_count > MAX_LOG_LINES:
                        self.log_text.delete("1.0", "500.0")
                        self._log_line_count -= 500
                    self.log_text.see(tk.END)
                    self.log_text.config(state="disabled")
                elif kind == "stopped":
                    self.start_button.config(state="normal")
                    self.stop_button.config(state="disabled")
                    self.status_var.set("\u25cf Stopped" if payload in (0, None)
                                       else f"\u25cf Stopped (exit code {payload})")
                    self.status_label.config(foreground="#888888")
                elif kind == "error":
                    self.start_button.config(state="normal")
                    self.stop_button.config(state="disabled")
                    self.status_var.set("\u25cf Error")
                    self.status_label.config(foreground="#c0392b")
                    messagebox.showerror("Error", f"Could not start generator:\n{payload}")
        except queue.Empty:
            pass
        self.root.after(100, self._poll_queue)


if __name__ == "__main__":
    root = tk.Tk()
    app = GeneratorGUI(root)
    root.mainloop()
