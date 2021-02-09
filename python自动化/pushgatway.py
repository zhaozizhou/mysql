import socket, sys
import time
import argparse
from argparse import RawTextHelpFormatter
import prometheus_client
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway


