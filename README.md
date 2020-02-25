# node-hermes-protocol

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