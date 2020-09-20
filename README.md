# Installation

To clone this repo:

```
git clone git@github.com:vicjor/TDT4225_Assignment2.git
```

Due to its size, the `dataset/` folder is gitignored for this project. Download the `dataset.zip` file from Blackboard, and unzip it inside this repo.

## Initialize a virtual environment

To run the correct version of Python, and to install all the required packages, create a virtual environment inside this repo.

Install virtualenv if you don't already have it on your machine.

```
python3 -m pip install --user virtualenv
```

The, initialize a virtual environment:

```
virtualenv venv
```

After initializing a virtual environment, activate it with this command

```
. venv/bin/activate
```

finally, install all the required packages to the virtual environment with the following command

```
pip install -r requirements.txt
```

## Connecting to the database on the VM and running Python code example

You must either be logged in on eduroam, or connected to NTNUs network through Cisco VPN to run these files. After that, you can simply run the `example.py` file.
