# node-hermes-protocol

[![npm version](https://badge.fury.io/js/%40mathquis%2Fnode-hermes-protocol.svg)](https://badge.fury.io/js/%40mathquis%2Fnode-hermes-protocol)

Hermes protocol for Node.js using MQTT


### Installation

```bash
npm i @mathquis/node-hermes-protocol
```

### Usage

```javascript
const Hermes = require('@mathquis/node-hermes-protocol')

const client = Hermes({
	mqtt: {
		host: 'mqtt://localhost:1883'
	}
})
```