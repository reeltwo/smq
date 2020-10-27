#include <stdio.h>
#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include "smq.h"

// Module method definitions
static PyObject* py_init(PyObject *self, PyObject *args)
{
    int r = smq_init();
    Py_RETURN_NONE;
}

static PyObject* py_spin(PyObject *self, PyObject *args)
{
    smq_spin();
    Py_RETURN_NONE;
}

static PyObject* py_spin_once(PyObject *self, PyObject *args)
{
    smq_spin_once(10);
    Py_RETURN_NONE;
}

// Module method definitions
static PyObject* py_advertise(PyObject *self, PyObject *args)
{
    const char* topic;
    if (!PyArg_ParseTuple(args, "s", &topic))
    {
        return NULL;
    }
    smq_advertise(topic);
    Py_RETURN_NONE;
}

static PyObject* py_advertise_hash(PyObject *self, PyObject *args)
{
    int r;
    const char* topic;
    if (!PyArg_ParseTuple(args, "s", &topic))
    {
        return NULL;
    }
    r = smq_advertise_hash(topic);
    Py_RETURN_NONE;
}

static void py_message_callback(const char* topic, const uint8_t* msg, size_t len, void* arg)
{
    Py_ssize_t msglen = len;
    PyObject* callback = (PyObject*)arg;
    PyObject* arglist = Py_BuildValue("(ss#)", topic, (char*)msg, msglen);
    PyObject* result = PyEval_CallObject(callback, arglist);
    Py_XDECREF(result);
    Py_DECREF(arglist);
}

static PyObject* py_subscribe(PyObject *self, PyObject *args)
{
    const char* topic;
    PyObject* pycallback;
    if (!PyArg_ParseTuple(args, "sO", &topic, &pycallback))
    {
        return NULL;
    }
    if (!PyCallable_Check(pycallback))
    {
        PyErr_SetString(PyExc_TypeError, "Must specify a message handler function!");
    }
    else
    {
        Py_INCREF(pycallback);
        smq_subscribe(topic, py_message_callback, pycallback);
    }
    Py_RETURN_NONE;
}

static PyObject* py_subscribe_hash(PyObject *self, PyObject *args)
{
    const char* topic;
    PyObject* pycallback;
    if (!PyArg_ParseTuple(args, "sO", &topic, &pycallback))
    {
        return NULL;
    }
    if (!PyCallable_Check(pycallback))
    {
        PyErr_SetString(PyExc_TypeError, "Must specify a message handler function!");
    }
    else
    {
        Py_INCREF(pycallback);
        smq_subscribe_hash(topic, py_message_callback, pycallback);
    }
    Py_RETURN_NONE;
}

static PyObject* py_publish(PyObject *self, PyObject *args)
{
    const char* topic;
    const char* json;
    if (!PyArg_ParseTuple(args, "ss", &topic, &json))
    {
        return NULL;
    }
    smq_publish(topic, (uint8_t*)json, strlen(json));
    Py_RETURN_NONE;
}

static PyObject* py_publish_hash(PyObject *self, PyObject *args)
{
    const char* topic;
    const char* json;
    if (!PyArg_ParseTuple(args, "ss", &topic, &json))
    {
        return NULL;
    }
    smq_publish_hash(topic, (uint8_t*)json, strlen(json));
    Py_RETURN_NONE;
}

// Method definition object for this extension, these argumens mean:
// ml_name: The name of the method
// ml_meth: Function pointer to the method implementation
// ml_flags: Flags indicating special features of this method, such as
//          accepting arguments, accepting keyword arguments, being a
//          class method, or being a static method of a class.
// ml_doc:  Contents of this method's docstring
static PyMethodDef pysmq_methods[] =
{
    {
        "init", py_init, METH_NOARGS,
        "Initialize SMQ."
    },
    {
        "spin", py_spin, METH_NOARGS,
        "Wait for network events."
    },
    {
        "spin_once", py_spin_once, METH_NOARGS,
        "Check for network events."
    },
    {
        "advertise", py_advertise, METH_VARARGS,
        "Advertise the specified topic on the network."
    },
    {
        "advertise_hash", py_advertise_hash, METH_VARARGS,
        "Advertise the specified topic hash on the network."
    },
    {
        "subscribe", py_subscribe, METH_VARARGS,
        "Subscribe to the specified topic on the network."
    },
    {
        "subscribe_hash", py_subscribe_hash, METH_VARARGS,
        "Subscribe to the specified topic hash on the network."
    },
    {
        "publish", py_publish, METH_VARARGS,
        "Publish the a message for the specified topic."
    },
    {
        "publish_hash", py_publish_hash, METH_VARARGS,
        "Publish the a message for the specified topic hash."
    },
    { NULL, NULL, 0, NULL }
};

// Module definition
// The arguments of this structure tell Python what to call your extension,
// what it's methods are and where to look for it's method definitions
static struct PyModuleDef pysmq_definition =
{
    PyModuleDef_HEAD_INIT,
    "pysmq",
    "Python binding for SMQ.",
    -1, 
    pysmq_methods
};

// Module initialization
// Python calls this function when importing your extension. It is important
// that this function is named PyInit_[[your_module_name]] exactly, and matches
// the name keyword argument in setup.py's setup() call.
PyMODINIT_FUNC PyInit_pysmq(void)
{
    Py_Initialize();
    return PyModule_Create(&pysmq_definition);
}
