# SDLE Project

SDLE Project for group T2G15.

Group members:

1. José Rocha (up201806371@edu.fe.up.pt)
2. Miguel Silva (up201806388@edu.fe.up.pt)
3. Pedro Ferreira (up201806506@edu.fe.up.pt)
4. Pedro Ponte (up201809694@edu.fe.up.pt)



## Running the Project

Since the project is based on RMI, the first step for running the project is to start the RMI Registry. The RMI Registry is automatically binded by either the first Publisher or Subscriber that connects to the network. There are 4 main classes:

- RMI - this is where the RMI Registry is binded.
- Proxy
- Publisher
- Subscriber
- TestApp - this is where the protocols will be ran.

#### RMI

```
java -jar rmi.jar
```

#### Proxy

```
java -jar proxy.jar
```

#### Publisher

```
java -jar publisher.jar <id>
```

E.g.: java -jar publisher.jar 1

#### Subscriber

```
java -jar subscriber.jar <id>
```

E.g.: java -jar subscriber.jar 1

#### TestApp

```
java -jar testapp.jar <protocol> <pub/sub id> [<arg1>] [<arg2>]
```

E.g.: 

java -jar testapp.jar subscribe 1 [TestTopic]

java -jar testapp.jar unsubscribe 1 [TestTopic]

java -jar testapp.jar put 1 [TestTopic] [This is a test message.]

java -jar testapp.jar get 1 [TestTopic]



It is worth nothing that these .jar files can be found on the root of our project, under the folder "jar".