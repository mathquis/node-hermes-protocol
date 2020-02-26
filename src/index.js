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
			logger.info('Reconnecting to MQTT broker "%s"...', MQTT_HOST)
		})
		.on('close', () => {
			logger.info('Disconnected from MQTT broker "%s"', MQTT_HOST)
		})
		.on('disconnect', packet => {
			logger.info('Disconnecting from MQTT broker "%s"...', MQTT_HOST)
		})
		.on('offline', () => {
			logger.info('MQTT client is offline')
		})
		.on('error', err => {
			onError(err)
		})
		.on('end', () => {
			logger.info('Terminated connection to MQTT broker "%s"...', MQTT_HOST)
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
		dialogue: {
			load: async () => {
				logger.info('Dialogue is loaded')
				await client.publish(topics.DIALOGUE_LOAD, serialize({}))
			},
			onLoad: handler => {
				return on(topics.DIALOGUE_LOAD, handler)
			},
			startSession: async (siteId, init, customData) => {
				logger.info('Starting session on site "%s"', siteId)
				await publish(topics.DIALOGUE_START_SESSION, serialize({
					siteId, init, customData
				}))
			},
			onStartSession: handler => {
				return on(topics.DIALOGUE_START_SESSION, handler)
			},
			sessionStarted: async (siteId, sessionId, customData) => {
				logger.info('Session "%s" started on site "%s"', sessionId, siteId)
				await publish(topics.DIALOGUE_SESSION_STARTED, serialize({
					siteId, sessionId, customData
				}))
			},
			onSessionStarted: handler => {
				return on(topics.DIALOGUE_SESSION_STARTED, handler)
			},
			continueSession: async (siteId, sessionId, text, customData, intentFilter, sendIntentNotRecognized, slot) => {
				logger.info('Continuing session "%s" on site "%s"', sessionId, siteId)
				await publish(topics.DIALOGUE_CONTINUE_SESSION, serialize({
					siteId, sessionId, text, customData, intentFilter, sendIntentNotRecognized, slot
				}))
			},
			onContinueSession: handler => {
				return on(topics.DIALOGUE_CONTINUE_SESSION, handler)
			},
			endSession: async (siteId, sessionId, text, customData) => {
				logger.info('Ending session "%s" on site "%s"', sessionId, siteId)
				await publish(topics.DIALOGUE_END_SESSION, serialize({
					siteId, sessionId, text, customData
				}))
			},
			onEndSession: handler => {
				return on(topics.DIALOGUE_END_SESSION, handler)
			},
			sessionEnded: async (siteId, sessionId, customData, reason) => {
				logger.info('Session "%s" ended on site "%s" because "%s"', sessionId, siteId, reason)
				await publish(topics.DIALOGUE_SESSION_ENDED, serialize({
					siteId, sessionId, customData, termination: {reason}
				}))
			},
			onSessionEnded: handler => {
				return on(topics.DIALOGUE_SESSION_ENDED, handler)
			},
			intent: async (siteId, sessionId, input, intent, slots, asrTokens, asrConfidence, alternatives, customData) => {
				logger.info('Recognized intent "%s" for session "%s" on site "%s"', intent.intentName, sessionId, siteId)
				await publish('hermes/intent/' + intent.intentName, serialize({
					siteId, sessionId, input, intent, slots, asrTokens, asrConfidence, alternatives, customData
				}))
			},
			onIntent: (intentName, handler) => {
				return on(format(topics.DIALOGUE_INTENT, {intentName}), handler)
			},
			intentNotRecognized: async (siteId, sessionId, input, customData) => {
				logger.info('Intent not recognized for session "%s" on site "%s"', sessionId, siteId)
				await publish(topics.DIALOGUE_INTENT_NOT_RECOGNIZED, serialize({
					siteId, sessionId, input, customData
				}))
			},
			onIntentNotRecognized: handler => {
				return on(topics.DIALOGUE_INTENT_NOT_RECOGNIZED, handler)
			}
		},
		feedback: {
			sound: {
				toggleOn: async (siteId) => {
					logger.info('Toggling feedback sound "On" on site "%s"', siteId)
					await publish(topics.FEEDBACK_SOUND_TOGGLE_ON, serialize({
						siteId
					}))
				},
				onToggleOn: handler => {
					return on(topics.FEEDBACK_SOUND_TOGGLE_ON,  handler)
				},
				toggleOff: async (siteId) => {
					logger.info('Toggling feedback sound "Off" on site "%s"', siteId)
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
				logger.info('Hotword is loaded on site "%s"', siteId)
				await client.publish(topics.HOTWORD_LOAD, serialize({siteId}))
			},
			toggleOn: async (siteId, sessionId) => {
				logger.info('Toggling hotword "On" on site "%s"', siteId)
				await publish(topics.HOTWORD_TOGGLE_ON, serialize({
					siteId, sessionId
				}))
			},
			onToggleOn: handler => {
				return on(topics.HOTWORD_TOGGLE_ON, handler)
			},
			toggleOff: async (siteId, sessionId) => {
				logger.info('Toggling hotword "Off" on site "%s"', siteId)
				await publish(topics.HOTWORD_TOGGLE_OFF, serialize({
					siteId, sessionId
				}))
			},
			onToggleOff: handler => {
				return on(topics.HOTWORD_TOGGLE_OFF, handler)
			},
			detected: async (siteId, modelId, modelVersion, modelType, currentSensitivity) => {
				logger.info('Hotword "%s" detected on site "%s"', modelId, siteId)
				await publish(format(topics.HOTWORD_DETECTED, {modelId}), serialize({
					siteId, modelId, modelVersion, modelType, currentSensitivity
				}))
			},
			onDetected: handler => {
				return on(format(topics.HOTWORD_DETECTED, {modelId: '+'}), handler)
			}
		},
		asr: {
			load: async () => {
				logger.info('ASR is loaded')
				await client.publish(topics.ASR_LOAD, serialize({}))
			},
			onLoad: handler => {
				return on(topics.ASR_LOAD, handler)
			},
			toggleOn: async (siteId, sessionId) => {
				logger.info('Toggling ASR "On" on site "%s"', siteId)
				await publish(topics.ASR_TOGGLE_ON)
			},
			onToggleOn: handler => {
				return on(topics.ASR_TOGGLE_ON, handler)
			},
			toggleOff: async (siteId, sessionId) => {
				logger.info('Toggling ASR "Off" on site "%s"', siteId)
				await publish(topics.ASR_TOGGLE_OFF)
			},
			onToggleOff: handler => {
				return on(topics.ASR_TOGGLE_OFF, handler)
			},
			startListening: async (siteId, sessionId) => {
				logger.info('Start listening ASR for session "%s" on site "%s"', sessionId, siteId)
				await publish(topics.ASR_START_LISTENING, serialize({
					siteId, sessionId
				}))
			},
			onStartListening: handler => {
				return on(topics.ASR_START_LISTENING, handler)
			},
			stopListening: async (siteId, sessionId) => {
				logger.info('Stop listening ASR for session "%s" on site "%s"', sessionId, siteId)
				await publish(topics.ASR_STOP_LISTENING, serialize({
					siteId, sessionId
				}))
			},
			onStopListening: handler => {
				return on(topics.ASR_STOP_LISTENING, handler)
			},
			textCaptured: async (siteId, sessionId, text, likelihood, seconds) => {
				logger.info('ASR captured text "%s" for session "%s" on site "%s"', text, sessionId, siteId)
				await publish(topics.ASR_TEXT_CAPTURED, serialize({
					siteId, sessionId, text, likelihood, seconds
				}))
			},
			onTextCaptured: handler => {
				return on(topics.ASR_TEXT_CAPTURED, handler)
			},
			error: async (err) => {
				logger.error('ASR error:', err)
				await publish(topics.ASR_ERROR, serialize({
					error: err
				}))
			},
			onError: handler => {
				return on(topics.ASR_ERROR, handler)
			}
		},
		nlu: {
			load: async () => {
				logger.info('NLU is loaded')
				await client.publish(topics.NLU_LOAD, serialize({}))
			},
			onLoad: handler => {
				return on(topics.NLU_LOAD, handler)
			},
			query: async (sessionId, input, intentFilter, id) => {
				logger.info('Querying NLU with "%s" for session "%s"', input, sessionId)
				await publish(topics.NLU_QUERY, serialize({
					sessionId, input, intentFilter, id
				}))
			},
			onQuery: handler => {
				return on(topics.NLU_QUERY, handler)
			},
			intentParsed: async (id, intent, input, slots, sessionId) => {
				logger.info('Request "%s" parsed intent "%s" parsed from input "%s" for session "%s"', id, intent.intentName, input, sessionId)
				await publish('hermes/nlu/intentParsed', serialize({
					id, intent, input, slots, sessionId
				}))
			},
			onIntentParsed: handler => {
				return on(topics.NLU_INTENT_PARSED, handler)
			},
			intentNotRecognized: async (id, input, sessionId) => {
				logger.info('Request "%s" did not recognized intent for input "%s" for session "%s"', id, input, sessionId)
				await publish('hermes/nlu/intentNotRecognized', serialize({
					id, input, sessionId
				}))
			},
			onIntentNotRecognized: handler => {
				return on(topics.NLU_INTENT_NOT_RECOGNIZED, handler)
			},
			error: async (err) => {
				logger.error('NLU error:', err)
				await publish('hermes/nlu/error', serialize({
					error: err
				}))
			},
			onError: handler => {
				return on(topics.NLU_ERROR, handler)
			}
		},
		tts: {
			load: async () => {
				logger.info('TTS is loaded')
				await client.publish(topics.TTS_LOAD, serialize({}))
			},
			onLoad: handler => {
				return on(topics.TTS_LOAD, handler)
			},
			say: async (siteId, sessionId, text, lang, timeout) => {
				logger.info('Speaking "%s" for session "%s" on site "%s"', text, sessionId, siteId)
				let p
				if ( timeout ) p = hermes.tts.waitForSayFinished(siteId, sessionId, timeout)
				client.publish(topics.TTS_SAY, serialize({
					siteId, sessionId, text, lang
				}))
				await p
			},
			onSay: handler => {
				return on(topics.TTS_SAY, handler)
			},
			sayFinished: async (siteId, sessionId) => {
				logger.info('Text spoken for session "%s" on site "%s"', sessionId, siteId)
				await publish(topics.TTS_SAY_FINISHED, serialize({
					siteId, sessionId
				}))
			},
			onSayFinished: handler => {
				return on(topics.TTS_SAY_FINISHED, handler)
			},
			waitForSayFinished: (siteId, id, timeout) => {
				return waitFor(topics.TTS_SAY_FINISHED, (topic, payload) => {
					if ( payload.id != id ) return
					logger.info('Speaking "%s" finished on site "%s"', id, siteId)
					return true
				}, timeout)
			},
			error: async (err) => {
				logger.error('TTS error:', err)
				await publish(topics.TTS_ERROR, serialize({
					error: err
				}))
			},
			onError: handler => {
				return on(topics.TTS_ERROR, handler)
			}
		},
		audioServer: {
			load: async (siteId) => {
				logger.info('Audio server is loaded')
				await client.publish(topics.AUDIO_SERVER_LOAD, serialize({siteId}))
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
					logger.info('Playing "%s" finished on site "%s"', id, siteId)
					return true
				}, timeout)
			},
			playFinished: async (siteId, id) => {
				logger.info('Playing "%s" finished on site "%s"', id, siteId)
				await publish(format(topics.AUDIO_SERVER_PLAY_FINISHED, {siteId}), serialize({
					id, siteId
				}))
			},
			onPlayFinished: (siteId, handler) => {
				on(format(topics.AUDIO_SERVER_PLAY_FINISHED, {siteId}), handler)
			},
			playBytesStream: async (siteId, sessionId, id, chunk, index, isLastChunk, timeout) => {
				id || (id = sessionId)
				isLastChunk = !!isLastChunk ? '1' : '0'
				let p
				if ( isLastChunk ) {
					const p = hermes.waitForStreamFinished(siteId, id, timeout)
				}
				await publish(format(topics.AUDIO_SERVER_PLAY_BYTES_STREAM, {siteId, id, index, isLastChunk}), chunk)
				await p
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
					logger.info('Streaming "%s" finished on site "%s"', id, siteId)
					return true
				}, timeout)
			},
			streamFinished: async (siteId, id) => {
				logger.info('Streaming "%s" finished on site "%s"', id, siteId)
				await publish(format(topics.AUDIO_SERVER_STREAM_FINISHED, {siteId}), serialize({
					id, siteId
				}))
			},
			onStreamFinished: (siteId, handler) => {
				on(format(topics.AUDIO_SERVER_STREAM_FINISHED, {siteId}), handler)
			},
			error: async (err) => {
				logger.error('Audio server error:', err)
				await publish(topics.AUDIO_SERVER_ERROR, serialize({
					siteId,
					error: err
				}))
			},
			onError: handler => {
				return on(topics.AUDIO_SERVER_ERROR, handler)
			}
		},
		injection: {
			perform: async (id, crossLanguage, lexicon, operation, timeout) => {
				logger.info('TODO: Performing injection')
				let p
				if ( timeout ) p = hermes.waitForInjectionComplete(id, timeout)
				await publish('hermes/injection/perform', serialize({
					id, crossLanguage, lexicon, operation
				}))
				await p
			},
			performComplete: async (requestId) => {
				logger.info('Injection "%s" complete', requestId)
				await publish('hermes/injection/complete', serialize({
					requestId
				}))
			},
			waitForPerformComplete: (requestId, timeout) => {
				logger.debug('Waiting for injection "%s" completed for %d ms', requestId, timeout)
				return waitFor('hermes/injection/complete', (topic, message) => {
					const payload = unserialize(message)
					if ( payload.requestId != requestId ) return
					logger.info('Injection "%s" complete', requestId, siteId)
					return true
				}, timeout)
			},
			reset: async (id, crossLanguage, lexicon, operation, timeout) => {
				logger.info('TODO: Injection resetting')
				let p
				if ( timeout ) p = hermes.waitForInjectionComplete(id, timeout)
				await publish('hermes/injection/reset', serialize({
					id, crossLanguage, lexicon, operation
				}))
				await p
			},
			resetComplete: async (requestId) => {
				logger.info('Injection "%s" reset', requestId)
				await publish('hermes/injection/reset/complete', serialize({
					requestId
				}))
			},
			waitForResetComplete: (requestId, timeout) => {
				logger.debug('Waiting for injection "%s" completed for %d ms', requestId, timeout)
				return waitFor('hermes/injection/reset/complete', (topic, message) => {
					const payload = unserialize(message)
					if ( payload.requestId != requestId ) return
					logger.info('Injection "%s" reset', requestId, siteId)
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
			topic
				.replace(/[+]/g, '[^\/]+') // One word
				.replace(/[#]/g, '.*') // Any word
		)
		const listeners = handlers.get(topic) || []
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
				resolve()
			})
			listener.timeout = setTimeout(() => {
				listener.remove()
				reject(new Error('Wait timeout'))
			}, timeout || 30000)
		})
	}

	return hermes
}