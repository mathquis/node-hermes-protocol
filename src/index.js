const MQTT		= require('mqtt')
const topics	= require('./topics')

module.exports = (options) => {
	options || (options = {})

	const MQTT_HOST = options.host || 'mqtt://localhost:1883'

	const logger = options.logger || console

	const client = MQTT.connect( MQTT_HOST, options )

	client
		.on('connect', () => {
			logger.info('Connected to MQTT broker "%s"', MQTT_HOST)
			handlers.forEach((handler, topic) => {
				client.subscribe(topic)
			})
		})
		.on('reconnect', () => {
			logger.debug('Reconnecting to MQTT broker "%s"...', MQTT_HOST)
		})
		.on('close', () => {
			logger.info('Disconnected from MQTT broker "%s"', MQTT_HOST)
		})
		.on('disconnect', packet => {
			logger.debug('Disconnecting from MQTT broker "%s"...', MQTT_HOST)
		})
		.on('offline', () => {
			logger.debug('MQTT client is offline')
		})
		.on('error', err => {
			logger.error(err.message)
		})
		.on('end', () => {
			logger.info('Terminated connection to MQTT broker "%s"', MQTT_HOST)
		})
		.on('message', (t, m) => {
			logger.debug('Received topic "%s"', t)
			handlers.forEach((handler, topic) => {
				let match
				if ( match = t.match(handler.reg) ) {
					handler.listeners.forEach(listener => listener(t, m))
				}
			})
		})

	const handlers = new Map()

	const hermes = {
		// Helpers
		on, publish, format, waitFor, serialize, unserialize, noop,

		// Dialogue
		dialogue: {
			load: async () => {
				logger.debug('Dialogue is loaded')
				await publish(topics.DIALOGUE_LOAD, serialize({}))
			},
			onLoad: handler => {
				return on(topics.DIALOGUE_LOAD, handler)
			},
			startSession: async (siteId, init, customData) => {
				logger.debug('Starting session on site "%s"', siteId)
				await publish(topics.DIALOGUE_START_SESSION, serialize({
					siteId, init, customData
				}))
			},
			startActionSession: async (siteId, text, canBeQueued, intentFilter, sendIntentNotRecognized) => {
				logger.debug('Starting session on site "%s"', siteId)
				await publish(topics.DIALOGUE_START_SESSION, serialize({
					siteId, init: {
						type: 'action',
						text, canBeQueued, intentFilter, sendIntentNotRecognized
					}, customData
				}))
			},
			startNotificationSession: async (siteId, text, customData) => {
				logger.debug('Starting session on site "%s"', siteId)
				await publish(topics.DIALOGUE_START_SESSION, serialize({
					siteId, init: {type: 'notification', text}, customData
				}))
			},
			onStartSession: handler => {
				return on(topics.DIALOGUE_START_SESSION, handler)
			},
			sessionStarted: async (siteId, sessionId, customData) => {
				logger.debug('Session "%s" started on site "%s"', sessionId, siteId)
				await publish(topics.DIALOGUE_SESSION_STARTED, serialize({
					siteId, sessionId, customData
				}))
			},
			onSessionStarted: handler => {
				return on(topics.DIALOGUE_SESSION_STARTED, handler)
			},
			continueSession: async (siteId, sessionId, text, customData, intentFilter, sendIntentNotRecognized, slot) => {
				logger.debug('Continuing session "%s" on site "%s"', sessionId, siteId)
				await publish(topics.DIALOGUE_CONTINUE_SESSION, serialize({
					siteId, sessionId, text, customData, intentFilter, sendIntentNotRecognized, slot
				}))
			},
			onContinueSession: handler => {
				return on(topics.DIALOGUE_CONTINUE_SESSION, handler)
			},
			endSession: async (siteId, sessionId, text, customData) => {
				logger.debug('Ending session "%s" on site "%s"', sessionId, siteId)
				await publish(topics.DIALOGUE_END_SESSION, serialize({
					siteId, sessionId, text, customData
				}))
			},
			onEndSession: handler => {
				return on(topics.DIALOGUE_END_SESSION, handler)
			},
			sessionQueued: async (siteId, sessionId, text, customData) => {
				logger.debug('Queued session "%s" on site "%s"', sessionId, siteId)
				await publish(topics.DIALOGUE_SESSION_QUEUED, serialize({
					siteId, sessionId, text, customData
				}))
			},
			onSessionQueued: handler => {
				return on(topics.DIALOGUE_SESSION_QUEUED, handler)
			},
			sessionEnded: async (siteId, sessionId, customData, reason) => {
				logger.debug('Session "%s" ended on site "%s" because "%s"', sessionId, siteId, reason)
				await publish(topics.DIALOGUE_SESSION_ENDED, serialize({
					siteId, sessionId, customData, termination: {reason}
				}))
			},
			onSessionEnded: handler => {
				return on(topics.DIALOGUE_SESSION_ENDED, handler)
			},
			intent: async (siteId, sessionId, input, intent, slots, asrTokens, asrConfidence, alternatives, customData) => {
				logger.debug('Recognized intent "%s" for session "%s" on site "%s"', intent.intentName, sessionId, siteId)
				await publish('hermes/intent/' + intent.intentName, serialize({
					siteId, sessionId, input, intent, slots, asrTokens, asrConfidence, alternatives, customData
				}))
			},
			onIntent: (intentName, handler) => {
				return on(format(topics.DIALOGUE_INTENT, {intentName}), handler)
			},
			intentNotRecognized: async (siteId, sessionId, input, customData) => {
				logger.debug('Intent not recognized for session "%s" on site "%s"', sessionId, siteId)
				await publish(topics.DIALOGUE_INTENT_NOT_RECOGNIZED, serialize({
					siteId, sessionId, input, customData
				}))
			},
			onIntentNotRecognized: handler => {
				return on(topics.DIALOGUE_INTENT_NOT_RECOGNIZED, handler)
			},
			error: async (err, context) => {
				logger.error('Dialogue error:', err)
				await publish(topics.DIALOGUE_ERROR, serialize({
					error: err,
					context
				}))
			},
			onError: handler => {
				return on(topics.DIALOGUE_ERROR, handler)
			}
		},
		feedback: {
			sound: {
				toggleOn: async (siteId) => {
					logger.debug('Toggling feedback sound "On" on site "%s"', siteId)
					await publish(topics.FEEDBACK_SOUND_TOGGLE_ON, serialize({
						siteId
					}))
				},
				onToggleOn: handler => {
					return on(topics.FEEDBACK_SOUND_TOGGLE_ON,  handler)
				},
				toggleOff: async (siteId) => {
					logger.debug('Toggling feedback sound "Off" on site "%s"', siteId)
					await publish(topics.FEEDBACK_SOUND_TOGGLE_OFF, serialize({
						siteId
					}))
				},
				onToggleOff: handler => {
					return on(topics.FEEDBACK_SOUND_TOGGLE_OFF,  handler)
				}
			}
		},
		hotword: {
			load: async (siteId) => {
				logger.debug('Hotword is loaded on site "%s"', siteId)
				await publish(topics.HOTWORD_LOAD, serialize({siteId}))
			},
			toggleOn: async (siteId, sessionId) => {
				logger.debug('Toggling hotword "On" on site "%s"', siteId)
				await publish(topics.HOTWORD_TOGGLE_ON, serialize({
					siteId, sessionId
				}))
			},
			onToggleOn: handler => {
				return on(topics.HOTWORD_TOGGLE_ON, handler)
			},
			toggleOff: async (siteId, sessionId) => {
				logger.debug('Toggling hotword "Off" on site "%s"', siteId)
				await publish(topics.HOTWORD_TOGGLE_OFF, serialize({
					siteId, sessionId
				}))
			},
			onToggleOff: handler => {
				return on(topics.HOTWORD_TOGGLE_OFF, handler)
			},
			detected: async (siteId, modelId, modelVersion, modelType, currentSensitivity) => {
				logger.debug('Hotword "%s" detected on site "%s"', modelId, siteId)
				await publish(format(topics.HOTWORD_DETECTED, {modelId}), serialize({
					siteId, modelId, modelVersion, modelType, currentSensitivity
				}))
			},
			onDetected: handler => {
				return on(format(topics.HOTWORD_DETECTED, {modelId: '+'}), handler)
			},
			error: async (siteId, err, context) => {
				logger.error('ASR error:', err)
				await publish(topics.HOTWORD_ERROR, serialize({
					siteId,
					error: err,
					context
				}))
			},
			onError: handler => {
				return on(topics.HOTWORD_ERROR, handler)
			}
		},
		asr: {
			load: async () => {
				logger.debug('ASR is loaded')
				await publish(topics.ASR_LOAD, serialize({}))
			},
			onLoad: handler => {
				return on(topics.ASR_LOAD, handler)
			},
			toggleOn: async (siteId, sessionId) => {
				logger.debug('Toggling ASR "On" on site "%s"', siteId)
				await publish(topics.ASR_TOGGLE_ON)
			},
			onToggleOn: handler => {
				return on(topics.ASR_TOGGLE_ON, handler)
			},
			toggleOff: async (siteId, sessionId) => {
				logger.debug('Toggling ASR "Off" on site "%s"', siteId)
				await publish(topics.ASR_TOGGLE_OFF)
			},
			onToggleOff: handler => {
				return on(topics.ASR_TOGGLE_OFF, handler)
			},
			startListening: async (siteId, sessionId) => {
				logger.debug('Start listening ASR for session "%s" on site "%s"', sessionId, siteId)
				await publish(topics.ASR_START_LISTENING, serialize({
					siteId, sessionId
				}))
			},
			onStartListening: handler => {
				return on(topics.ASR_START_LISTENING, handler)
			},
			stopListening: async (siteId, sessionId) => {
				logger.debug('Stop listening ASR for session "%s" on site "%s"', sessionId, siteId)
				await publish(topics.ASR_STOP_LISTENING, serialize({
					siteId, sessionId
				}))
			},
			onStopListening: handler => {
				return on(topics.ASR_STOP_LISTENING, handler)
			},
			textCaptured: async (siteId, sessionId, text, likelihood, seconds, tokens) => {
				logger.debug('ASR captured text "%s" for session "%s" on site "%s"', text, sessionId, siteId)
				await publish(topics.ASR_TEXT_CAPTURED, serialize({
					siteId, sessionId, text, likelihood, seconds, tokens
				}))
			},
			onTextCaptured: handler => {
				return on(topics.ASR_TEXT_CAPTURED, handler)
			},
			error: async (err, context) => {
				logger.error('ASR error:', err)
				await publish(topics.ASR_ERROR, serialize({
					error: err,
					context
				}))
			},
			onError: handler => {
				return on(topics.ASR_ERROR, handler)
			}
		},
		nlu: {
			load: async () => {
				logger.debug('NLU is loaded')
				await publish(topics.NLU_LOAD, serialize({}))
			},
			onLoad: handler => {
				return on(topics.NLU_LOAD, handler)
			},
			query: async (sessionId, input, intentFilter, id) => {
				logger.debug('Querying NLU with "%s" for session "%s"', input, sessionId)
				await publish(topics.NLU_QUERY, serialize({
					sessionId, input, intentFilter, id
				}))
			},
			onQuery: handler => {
				return on(topics.NLU_QUERY, handler)
			},
			intentParsed: async (sessionId, id, input, intent, slots) => {
				logger.debug('Request "%s" recognized intent "%s" from input "%s" for session "%s"', id, intent.intentName, input, sessionId)
				await publish('hermes/nlu/intentParsed', serialize({
					id, intent, input, slots, sessionId
				}))
			},
			onIntentParsed: handler => {
				return on(topics.NLU_INTENT_PARSED, handler)
			},
			intentNotRecognized: async (sessionId, id, input) => {
				logger.debug('Request "%s" did not recognized intent for input "%s" for session "%s"', id, input, sessionId)
				await publish('hermes/nlu/intentNotRecognized', serialize({
					id, input, sessionId
				}))
			},
			onIntentNotRecognized: handler => {
				return on(topics.NLU_INTENT_NOT_RECOGNIZED, handler)
			},
			error: async (err, context) => {
				logger.error('NLU error:', err)
				await publish('hermes/nlu/error', serialize({
					error: err,
					context
				}))
			},
			onError: handler => {
				return on(topics.NLU_ERROR, handler)
			}
		},
		tts: {
			load: async () => {
				logger.debug('TTS is loaded')
				await publish(topics.TTS_LOAD, serialize({}))
			},
			onLoad: handler => {
				return on(topics.TTS_LOAD, handler)
			},
			say: async (siteId, sessionId, id, text, lang, timeout) => {
				logger.debug('Speaking "%s" for session "%s" on site "%s"', text, sessionId, siteId)
				let p
				if ( timeout ) p = hermes.tts.waitForSayFinished(sessionId, id, timeout)
				publish(topics.TTS_SAY, serialize({
					siteId, sessionId, id, text, lang
				}))
				await p
			},
			onSay: handler => {
				return on(topics.TTS_SAY, handler)
			},
			sayFinished: async (sessionId, id) => {
				logger.debug('Speak request "%s" for session "%s" finished playing', id, sessionId)
				await publish(topics.TTS_SAY_FINISHED, serialize({
					sessionId, id
				}))
			},
			onSayFinished: handler => {
				return on(topics.TTS_SAY_FINISHED, handler)
			},
			waitForSayFinished: (sessionId, id, timeout) => {
				return waitFor(topics.TTS_SAY_FINISHED, (topic, payload) => {
					if ( payload.id != id ) return
					logger.debug('Speaking "%s" for session "%s" finished', id, sessionId)
					return true
				}, timeout)
			},
			error: async (err, context) => {
				logger.error('TTS error:', err)
				await publish(topics.TTS_ERROR, serialize({
					error: err,
					context
				}))
			},
			onError: handler => {
				return on(topics.TTS_ERROR, handler)
			}
		},
		audioServer: {
			load: async (siteId) => {
				logger.debug('Audio server is loaded')
				await publish(topics.AUDIO_SERVER_LOAD, serialize({siteId}))
			},
			audioFrame: async (siteId, chunk) => {
				logger.debug('Audio %d bytes frame on site "%s"', chunk.length, siteId)
				await publish(format(topics.AUDIO_SERVER_AUDIO_FRAME, {siteId}), noop(chunk))
			},
			onAudioFrame: (siteId, handler) => {
				logger.debug('Listening for audio frames on site "%s"', siteId)
				return on(format(topics.AUDIO_SERVER_AUDIO_FRAME, {siteId}), handler, noop)
			},
			playBytes: async (siteId, sessionId, id, bytes, timeout) => {
				id || (id = sessionId)
				logger.debug('Playing %d bytes for request "%s" on site "%s"', bytes.length, id, siteId)
				let p
				if ( timeout ) p = hermes.audioServer.waitForPlayFinished(siteId, id, timeout)
				await publish(format(topics.AUDIO_SERVER_PLAY_BYTES, {siteId, id}), noop(bytes))
				await p
			},
			onPlayBytes: (siteId, handler) => {
				const wrapper = (topic, payload) => {
					let match
					if ( match = topic.match(/^hermes\/audioServer\/([^\/]+)\/playBytes\/([^\/]+)$/)) {
						const [m, siteId, id] = match
						handler(topic, {siteId, id, bytes: payload})
					}
				}
				return on(format(topics.AUDIO_SERVER_PLAY_BYTES, {siteId, id: '+'}), wrapper, noop)
			},
			waitForPlayFinished: (siteId, id, timeout) => {
				logger.debug('Waiting for play "%s" finished on site "%s" for %d ms', id, siteId, timeout)
				return waitFor(format(topics.AUDIO_SERVER_PLAY_FINISHED, {siteId, id}), (topic, payload) => {
					if ( payload.id != id ) return
					logger.debug('Playing "%s" finished on site "%s"', id, siteId)
					return true
				}, timeout)
			},
			playFinished: async (siteId, id) => {
				logger.debug('Playing "%s" finished on site "%s"', id, siteId)
				await publish(format(topics.AUDIO_SERVER_PLAY_FINISHED, {siteId}), serialize({
					id, siteId
				}))
			},
			onPlayFinished: (siteId, handler) => {
				on(format(topics.AUDIO_SERVER_PLAY_FINISHED, {siteId}), handler)
			},
			playBytesStream: async (siteId, sessionId, id, chunk, index, isLastChunk, timeout) => {
				id || (id = sessionId)
				index = '' + index
				isLastChunk = !!isLastChunk ? '1' : '0'
				// let p
				// if ( isLastChunk ) {
				// 	const p = hermes.audioServer.waitForStreamFinished(siteId, id, timeout)
				// }
				await publish(format(topics.AUDIO_SERVER_PLAY_BYTES_STREAM, {siteId, id, index, isLastChunk}), chunk)
				// await p
			},
			onPlayBytesStream: (siteId, handler) => {
				const wrapper = (topic, payload) => {
					let match
					if ( match = topic.match(/^hermes\/audioServer\/([^\/]+)\/playBytesStreaming\/([^\/]+)\/([0-9]+)\/([01])$/)) {
						let [m, siteId, id, index, isLastChunk] = match
						index = parseInt(index)
						isLastChunk = isLastChunk === '1'
						handler(topic, {siteId, id, index, isLastChunk, bytes: payload})
					}
				}
				return on(format(topics.AUDIO_SERVER_PLAY_BYTES_STREAM, {siteId, id: '+', index: '+', isLastChunk: '+'}), wrapper, noop)
			},
			waitForStreamFinished: (siteId, id, timeout) => {
				logger.debug('Waiting for stream "%s" finished on site "%s" for %d ms', id, siteId, timeout)
				return waitFor(format(topics.AUDIO_SERVER_STREAM_FINISHED, {siteId}), (topic, payload) => {
					if ( payload.id != id ) return
					logger.debug('Streaming "%s" finished on site "%s"', id, siteId)
					return true
				}, timeout)
			},
			streamFinished: async (siteId, id) => {
				logger.debug('Streaming "%s" finished on site "%s"', id, siteId)
				await publish(format(topics.AUDIO_SERVER_STREAM_FINISHED, {siteId}), serialize({
					id, siteId
				}))
			},
			onStreamFinished: (siteId, handler) => {
				return on(format(topics.AUDIO_SERVER_STREAM_FINISHED, {siteId}), handler)
			},
			error: async (siteId, err, context) => {
				logger.error('Audio server error:', err)
				await publish(topics.AUDIO_SERVER_ERROR, serialize({
					siteId,
					error: err,
					context
				}))
			},
			onError: handler => {
				return on(topics.AUDIO_SERVER_ERROR, handler)
			}
		},
		injection: {
			perform: async (id, crossLanguage, lexicon, operations, timeout) => {
				logger.debug('Requesting injection operations')
				let p
				if ( timeout ) p = hermes.injection.waitForComplete(id, timeout)
				await publish(topics.INJECTION_PERFORM, serialize({
					id, crossLanguage, lexicon, operations
				}))
				await p
			},
			onPerform: handler => {
				return on(topics.INJECTION_PERFORM, handler)
			},
			complete: async (requestId) => {
				logger.debug('Injection "%s" complete', requestId)
				await publish(topics.INJECTION_COMPLETE, serialize({
					requestId
				}))
			},
			onComplete: (handler) => {
				return on(topics.INJECTION_COMPLETE, handler)
			},
			waitForComplete: (requestId, timeout) => {
				logger.debug('Waiting for injection "%s" completed for %d ms', requestId, timeout)
				return waitFor(topics.INJECTION_COMPLETE, (topic, payload) => {
					if ( payload.requestId != requestId ) return
					logger.debug('Injection "%s" complete', requestId)
					return true
				}, timeout)
			},
			reset: async (id, timeout) => {
				logger.debug('Requesting injection reset')
				let p
				if ( timeout ) p = hermes.injection.waitForResetComplete(id, timeout)
				await publish(topics.INJECTION_RESET_PERFORM, serialize({
					id
				}))
				await p
			},
			onReset: handler => {
				return on(topics.INJECTION_RESET_PERFORM, handler)
			},
			resetComplete: async (requestId) => {
				logger.debug('Injection "%s" reset', requestId)
				await publish(topics.INJECTION_RESET_COMPLETE, serialize({
					requestId
				}))
			},
			onResetComplete: handler => {
				return on(topics.INJECTION_RESET_COMPLETE, handler)
			},
			waitForResetComplete: (requestId, timeout) => {
				logger.debug('Waiting for injection "%s" completed for %d ms', requestId, timeout)
				return waitFor(topics.INJECTION_RESET_COMPLETE, (topic, payload) => {
					if ( payload.requestId != requestId ) return
					logger.debug('Injection "%s" reset', requestId, siteId)
					return true
				}, timeout)
			}
		}
	}

	function serialize(obj) {
		return JSON.stringify(obj)
	}

	function unserialize(data) {
		return JSON.parse(data)
	}

	function noop(data) {
		return data
	}

	function on(topic, handler, unpack) {
		unpack || (unpack = unserialize)
		const reg = new RegExp(
			'^'
			+ topic
				.replace(/[+]/g, '[^\/]+') // One word
				.replace(/[#]/g, '.*') // Any word
			+ '$'
		)
		const {listeners} = handlers.get(topic) || {listeners: []}
		const wrapper = (t, message) => {
			if ( !t.match(reg) ) return
			const payload = unpack(message)
			handler(t, payload)
		}
		wrapper.remove = () => {
			const pos = listeners.indexOf(wrapper)
			listeners.splice(pos, 1)
			handlers.set(topic, {reg, listeners})
			if ( listeners.length === 0 ) {
				client.unsubscribe(topic)
			}
		}
		listeners.push(wrapper)
		handlers.set(topic, {reg, listeners})
		client.subscribe(topic)
		return wrapper
	}

	async function publish(topic, message) {
		logger.debug('Publishing topic "%s"', topic)
		await client.publish(topic, message)
	}

	function format(str, obj) {
		return str.replace(/\\?\{([^\}]+)\}/gm, function (match, p1) {
			return obj[p1] || '';
		});
	}

	function waitFor(waitTopic, waitHandler, timeout) {
		return new Promise((resolve, reject) => {
			const listener = on(waitTopic, (topic, payload) => {
				if ( !waitHandler(topic, payload) ) return
				clearTimeout(listener.timeout)
				listener.remove()
				resolve(listener)
			})
			listener.timeout = setTimeout(() => {
				listener.remove()
				reject(new Error('Wait timeout'))
			}, timeout || 30000)
		})
	}

	return hermes
}