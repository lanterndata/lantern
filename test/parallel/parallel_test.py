import os
import yaml
import subprocess
import time
import atexit
from difflib import ndiff
from multiprocessing import Pool, Process, Queue

import psycopg2
from psycopg2 import sql

def build_tests(path='parallel.yaml'):
    with open(path, 'r') as cfg:
        parsed = yaml.safe_load(cfg)
        tests = [LDBTest(t) for t in parsed['tests']]
        return tests

class LDBTest:
    def __init__(self, yaml):
        self.name = yaml['name']
        self.steps = yaml['steps']

        self.tables = yaml['init']
        self.tables = ['sql/create/' + f + '.sql' for f in self.tables]

        self.units = yaml['units']
        self.units = ['sql/units/' + f + '.sql' for f in self.units]

        self.intra_invariants = yaml['intra-invariants']
        self.intra_expected = ['sql/expected/' + f + '.out' for f in self.intra_invariants]
        self.intra_invariants = ['sql/invariants/' + f + '.sql' for f in self.intra_invariants]

        self.invariants = yaml['invariants']
        self.expected = ['sql/expected/' + f + '.out' for f in self.invariants]
        self.invariants = ['sql/invariants/' + f + '.sql' for f in self.invariants]

        self.check_exists()

        self.env = {}
        self.get_env('DB_USER', 'user', 'postgres')
        self.get_env('DB_HOST', 'host', 'localhost')
        self.get_env('TEST_WORKERS', 'workers', 4)

        self.violations = []


    def get_env(self, key, name=None, default=None):
        val = os.environ.get(key)
        if name is None:
            name = key

        if val:
            self.env[name] = val
        else:
            if not default is None:
                self.env[name] = default
                return
            self.env[name] = None

    def check_exists(self):
        lists = [self.tables, self.units, self.intra_invariants, self.invariants, self.intra_expected, self.expected]
        for ll in lists:
            for f in ll:
                 if not os.path.isfile(f):
                     raise Exception(f'sql file {f} doesn\'t exist')

    def begin(self):
        con = psycopg2.connect(dbname='postgres',
                               user=self.env['user'],
                               host=self.env['host'])
        con.autocommit = True
        cur = con.cursor()
        cur.execute('CREATE DATABASE {};'.format(self.name))
        con.close()

        atexit.register(self.cleanup)

        for create in self.tables:
            subprocess.run(['psql','-d', self.name, '-h', self.env['host'],'-f', create], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def cleanup(self):
        con = psycopg2.connect(dbname='postgres',
                               user=self.env['user'],
                               host=self.env['host'])
        con.autocommit = True
        cur = con.cursor()
        cur.execute('DROP DATABASE IF EXISTS {};'.format(self.name))
        con.close()

    def run_tests(self):
        self.begin()

        bus = Queue()

        with Pool(self.env['workers']) as p:
            p_invariants = Process(target=self.check_invariants, args=(bus, True))
            p_invariants.start()
            p.map(self.run_test, self.units)
            bus.put('done')
            p_invariants.join()

        self.check_invariants(Queue(), False)
        self.cleanup()

    def run_test(self, sql):
        for _ in range(self.steps):
            subprocess.run(['psql','-d', self.name, '-h', self.env['host'], '-f', sql], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


    def check_invariants(self, bus, intra):
        if intra:
            invariants = self.intra_invariants
            outputs = self.intra_expected
        else:
            invariants = self.invariants
            outputs = self.expected

        while bus.empty():
            if not intra:
                bus.put('done')
            for sql, expected in zip(invariants, outputs): 
                p = subprocess.run(['psql','-d', self.name, '-h', self.env['host'],'-a', '-f', sql], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                out = p.stdout.decode()
                # TODO cache these
                with open(expected, 'r') as file:
                    expected_out = file.read()
                    if out != expected_out:
                        self.violations.append({
                            'invariant': sql,
                            'intra': intra,
                            'got': out,
                            'expected': expected_out,
                            'diff': ''.join(ndiff(out.splitlines(keepends=True), expected_out.splitlines(keepends=True)))
                        })

    def explain(self):
        intra = dict(zip(self.intra_invariants, ['ok'] * len(self.intra_invariants)))
        post = dict(zip(self.invariants, ['ok'] * len(self.invariants)))
        for v in self.violations:
            if v['intra']:
                intra[v['invariant']] = 'FAIL'
            else:
                post[v['invariant']] = 'FAIL'

        print('--------Invariants While Running--------')
        for k, v in intra.items():
            print(f'{k}: {v}')
        print('--------Invariants After Running--------')
        for k, v in post.items():
            print(f'{k}: {v}')

        print('----------------------------------------')
        for v in self.violations:
            print(f'ERROR: {v["invariant"]}')
            print(v['diff'])

    def pg(self):
        con = psycopg2.connect(dbname=self.name,
                               user=self.env['user'],
                               host='localhost')
        return con

if __name__ == '__main__':
    tests = build_tests('parallel.yaml')
    for test in tests:
        test.run_tests()
        test.explain()
