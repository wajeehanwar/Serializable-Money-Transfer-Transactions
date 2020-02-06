<h1 align="center">
  <br>
  Serializable Money Transfer Transactions
  <br>
</h1>

<h4 align="center">Transfer money using serializable transactions with retry logic for deadlocks and/or Oracle serialization errors.</h4>

<p align="center">
  <a href="#how-to-use">How To Use</a> •
  <a href="#credits">Credits</a> •
  <a href="#license">License</a>
</p>

## How To Use

For Oracle run with:

```bash
 $ java -classpath ojdbc6.jar;

 # Use ':' instead of ';' on UNIX

 # **Note**: change the variable oracleServer to "localhost"
 # for use with a tunnel.

 # **Note**: When we have multiple transactions in a program,
 # we should have each transaction in its own method, here
 # doTransfer, with the rollbacks and commits as needed so
 # that at the method return, there is no transaction running.
```

## Credits

This software was developed using the following:

- Java
- SQL
- Oracle

## License

MIT

---

> GitHub [@wajeehanwar](https://github.com/wajeehanwar)
