# Setting Up Your Environment

You need three things installed: **VS Code**, **Python**, and **uv**.

---

## 1. VS Code

**VS Code** (Visual Studio Code) is a free code editor from Microsoft. It's where you'll view and edit your pipeline files, and it has a built-in terminal for running commands.

- **Download it** from [code.visualstudio.com](https://code.visualstudio.com/) and run the installer
- During installation, **tick "Add to PATH"** and **tick "Register Code as an editor for supported file types"**
- Once installed, open it from the Start menu by searching for **Visual Studio Code**

---

## 2. Python

**Python** is the language Haute and your pricing pipeline are written in. You need version 3.11 or newer.

- **Download it** from [python.org/downloads](https://www.python.org/downloads/) and run the installer
- **Tick "Add Python to PATH"** during installation
- To check it worked, open the VS Code terminal (++ctrl+grave++) and type `python --version`

---

## 3. uv

**uv** is a fast Python package manager. It handles virtual environments and dependencies for you.

- Open the VS Code terminal and run: `powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"`
- To check it worked, type `uv --version`

See [uv installation docs](https://docs.astral.sh/uv/getting-started/installation/) if you run into trouble.

---

## Troubleshooting

**`command not found` when I type `uv` or `python`**

The tool isn't on your system PATH. Re-run its installer and make sure you tick **Add to PATH**. Close and reopen VS Code afterwards.

---

Once all three are installed, head to **[Installing Haute](installing-haute.md)**.
