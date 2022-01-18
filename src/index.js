const MQTT		= require('mqtt')
const UUID 		= require('uuid').v4
const Topics	= require('./topics')

module.exports = (options) => {
	options || (options = {})

	const handlers				= new Map()
	const connectHandlers		= []
	const disconnectHandlers	= []
	const queue					= []
	const logger				= options.logger || console
	const queueSize				= options.queueSize || 0

	const DialogInitTypes = {
		ACTION: 'action',
		NOTIFICATION: 'notification'
	}

	const InjectionTypes = {
		ADD: 'add',
		ADD_FROM_VANILLA: 'addFromVanilla'
	}

	let client

	const hermes = {
		Topics,

		connect: async (connectOptions) => {
			return new Promise((resolve, reject) => {
				if ( client ) return resolve()

				const MQTT_HOST = connectOptions.host || 'mqtt://localhost:1883'

				logger.debug('Connecting to MQTT broker "%s"...', MQTT_HOST)

				client = MQTT.connect( MQTT_HOST, {
					...options,
					...connectOptions
				})

				const cleanup = () => {
					client
						.off('connect', onSuccess)
						.off('error', onFailure)
				}
				const onSuccess = () => {
					cleanup()
					resolve()
				}
				const onFailure = err => {
					cleanup()
					reject(err)
				}

				client
					.once('connect', onSuccess)
					.once('error', onFailure)
					.on('connect', () => {
						logger.debug('Connected to MQTT broker "%s"', MQTT_HOST)
						client.off('error', reject)

						handlers.forEach((handler, topic) => {
							logger.debug('Subscribing to "%s"', topic)
							client.subscribe(topic)
						})
						// Publish queue messages
						if ( queue.length > 0 ) {
							logger.debug('Processing %d queued messages', queue.length)
							let queuedItem
							while ( queuedItem = queue.shift() ) {
								const {topic, message} = queuedItem
								publish(topic, message)
							}
						}

						connectHandlers.forEach(handler => handler())
					})
					.on('reconnect', () => {
						logger.debug('Reconnecting to MQTT broker "%s"...', MQTT_HOST)
					})
					.on('close', () => {
						logger.debug('Disconnected from MQTT broker "%s"', MQTT_HOST)
						disconnectHandlers.forEach(handler => handler())
					})
					.on('disconnect', packet => {
						logger.debug('Disconnecting from MQTT broker "%s"...', MQTT_HOST)
					})
					.on('offline', () => {
						logger.debug('MQTT client is offline')
					})
					.on('error', err => {
						logger.debug(err.message)
						disconnectHandlers.forEach(handler => handler(err))
					})
					.on('end', () => {
						logger.debug('Terminated connection to MQTT broker "%s"', MQTT_HOST)
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
			})
		},
		onConnect: handler => {
			connectHandlers.push(handler)
		},

		disconnect: () => {
			logger.debug('Disconnecting...')
			if ( client ) client.end()
			client = null
		},
		onDisconnect: handler => {
			disconnectHandlers.push(handler)
		},

		// Helpers
		on, publish, format, waitFor, waitForEither, serialize, unserialize, noop,

		wav: {
			create: createWav,
			read: readWav
		},

		// Dialogue
		dialogue: {
			types: DialogInitTypes,
			load: async () => {
				logger.debug('Dialogue is loaded')
				await publish(Topics.DIALOGUE_LOAD, serialize({}))
			},
			onLoad: handler => {
				return on(Topics.DIALOGUE_LOAD, handler)
			},
			startSession: async (siteId, init, customData) => {
				logger.debug('Starting session on site "%s"', siteId)
				await publish(Topics.DIALOGUE_START_SESSION, serialize({
					siteId, init, customData
				}))
			},
			startActionSession: async (siteId, text, canBeQueued, intentFilter, sendIntentNotRecognized, customData) => {
				logger.debug('Starting session on site "%s"', siteId)
				await publish(Topics.DIALOGUE_START_SESSION, serialize({
					siteId, init: {
						type: DialogInitTypes.ACTION,
						text, canBeQueued, intentFilter, sendIntentNotRecognized
					}, customData
				}))
			},
			startNotificationSession: async (siteId, text, customData) => {
				logger.debug('Starting session on site "%s"', siteId)
				await publish(Topics.DIALOGUE_START_SESSION, serialize({
					siteId, init: {
						type: DialogInitTypes.NOTIFICATION,
						text
					}, customData
				}))
			},
			onStartSession: handler => {
				return on(Topics.DIALOGUE_START_SESSION, handler)
			},
			sessionStarted: async (siteId, sessionId, customData) => {
				logger.debug('Session "%s" started on site "%s"', sessionId, siteId)
				await publish(Topics.DIALOGUE_SESSION_STARTED, serialize({
					siteId, sessionId, customData
				}))
			},
			onSessionStarted: handler => {
				return on(Topics.DIALOGUE_SESSION_STARTED, handler)
			},
			continueSession: async (siteId, sessionId, text, customData, intentFilter, sendIntentNotRecognized, slot) => {
				logger.debug('Continuing session "%s" on site "%s"', sessionId, siteId)
				await publish(Topics.DIALOGUE_CONTINUE_SESSION, serialize({
					siteId, sessionId, text, customData, intentFilter, sendIntentNotRecognized, slot
				}))
			},
			onContinueSession: handler => {
				return on(Topics.DIALOGUE_CONTINUE_SESSION, handler)
			},
			sessionContinued: async (siteId, sessionId, customData) => {
				logger.debug('Session "%s" continued on site "%s"', sessionId, siteId)
				await publish(Topics.DIALOGUE_SESSION_CONTINUED, serialize({
					siteId, sessionId, customData
				}))
			},
			onSessionContinued: handler => {
				return on(Topics.DIALOGUE_SESSION_CONTINUED, handler)
			},
			endSession: async (siteId, sessionId, text, customData) => {
				logger.debug('Ending session "%s" on site "%s"', sessionId, siteId)
				await publish(Topics.DIALOGUE_END_SESSION, serialize({
					siteId, sessionId, text, customData
				}))
			},
			onEndSession: handler => {
				return on(Topics.DIALOGUE_END_SESSION, handler)
			},
			sessionQueued: async (siteId, sessionId, text, customData) => {
				logger.debug('Queued session "%s" on site "%s"', sessionId, siteId)
				await publish(Topics.DIALOGUE_SESSION_QUEUED, serialize({
					siteId, sessionId, text, customData
				}))
			},
			onSessionQueued: handler => {
				return on(Topics.DIALOGUE_SESSION_QUEUED, handler)
			},
			sessionEnded: async (siteId, sessionId, customData, reason) => {
				logger.debug('Session "%s" ended on site "%s" because "%s"', sessionId, siteId, reason)
				await publish(Topics.DIALOGUE_SESSION_ENDED, serialize({
					siteId, sessionId, customData, termination: {reason}
				}))
			},
			onSessionEnded: handler => {
				return on(Topics.DIALOGUE_SESSION_ENDED, handler)
			},
			intent: async (siteId, sessionId, input, intentName, confidenceScore, slots, asrTokens, asrConfidence, alternatives, customData) => {
				logger.debug('Recognized intent "%s" for session "%s" on site "%s" with confidence %f', intentName, sessionId, siteId, confidenceScore)
				await publish('hermes/intent/' + intentName, serialize({
					siteId, sessionId, input, intent: {
						intentName, confidenceScore
					}, slots, asrTokens, asrConfidence, alternatives, customData
				}))
			},
			onIntent: (intentName, handler) => {
				return on(format(Topics.DIALOGUE_INTENT, {intentName}), handler)
			},
			intentNotRecognized: async (siteId, sessionId, input, customData) => {
				logger.debug('Intent not recognized for session "%s" on site "%s"', sessionId, siteId)
				await publish(Topics.DIALOGUE_INTENT_NOT_RECOGNIZED, serialize({
					siteId, sessionId, input, customData
				}))
			},
			onIntentNotRecognized: handler => {
				return on(Topics.DIALOGUE_INTENT_NOT_RECOGNIZED, handler)
			},
			error: async (err, context) => {
				logger.error('Dialogue error:', err)
				await publish(Topics.DIALOGUE_ERROR, serialize({
					error: err,
					context
				}))
			},
			onError: handler => {
				return on(Topics.DIALOGUE_ERROR, handler)
			}
		},
		feedback: {
			sound: {
				toggleOn: async (siteId) => {
					logger.debug('Toggling feedback sound "On" on site "%s"', siteId)
					await publish(Topics.FEEDBACK_SOUND_TOGGLE_ON, serialize({
						siteId
					}))
				},
				onToggleOn: handler => {
					return on(Topics.FEEDBACK_SOUND_TOGGLE_ON,  handler)
				},
				toggleOff: async (siteId) => {
					logger.debug('Toggling feedback sound "Off" on site "%s"', siteId)
					await publish(Topics.FEEDBACK_SOUND_TOGGLE_OFF, serialize({
						siteId
					}))
				},
				onToggleOff: handler => {
					return on(Topics.FEEDBACK_SOUND_TOGGLE_OFF,  handler)
				}
			}
		},
		hotword: {
			load: async (siteId) => {
				logger.debug('Hotword is loaded on site "%s"', siteId)
				await publish(Topics.HOTWORD_LOAD, serialize({siteId}))
			},
			toggleOn: async (siteId, sessionId) => {
				logger.debug('Toggling hotword "On" on site "%s"', siteId)
				await publish(Topics.HOTWORD_TOGGLE_ON, serialize({
					siteId, sessionId
				}))
			},
			onToggleOn: handler => {
				return on(Topics.HOTWORD_TOGGLE_ON, handler)
			},
			toggleOff: async (siteId, sessionId) => {
				logger.debug('Toggling hotword "Off" on site "%s"', siteId)
				await publish(Topics.HOTWORD_TOGGLE_OFF, serialize({
					siteId, sessionId
				}))
			},
			onToggleOff: handler => {
				return on(Topics.HOTWORD_TOGGLE_OFF, handler)
			},
			detected: async (siteId, modelId, modelVersion, modelType, confidence, currentSensitivity, detectionSignalMS, endSignalMS) => {
				logger.debug('Hotword "%s" detected on site "%s"', modelId, siteId)
				await publish(format(Topics.HOTWORD_DETECTED, {modelId}), serialize({
					siteId, modelId, modelVersion, modelType, confidence, currentSensitivity, detectionSignalMS, endSignalMS
				}))
			},
			onDetected: handler => {
				return on(format(Topics.HOTWORD_DETECTED, {modelId: '+'}), handler)
			},
			error: async (siteId, err, context) => {
				logger.error('ASR error:', err)
				await publish(Topics.HOTWORD_ERROR, serialize({
					siteId,
					error: err,
					context
				}))
			},
			onError: handler => {
				return on(Topics.HOTWORD_ERROR, handler)
			}
		},
		asr: {
			load: async () => {
				logger.debug('ASR is loaded')
				await publish(Topics.ASR_LOAD, serialize({}))
			},
			onLoad: handler => {
				return on(Topics.ASR_LOAD, handler)
			},
			toggleOn: async () => {
				logger.debug('Toggling ASR "On"')
				await publish(Topics.ASR_TOGGLE_ON, serialize({}))
			},
			onToggleOn: handler => {
				return on(Topics.ASR_TOGGLE_ON, handler)
			},
			toggleOff: async () => {
				logger.debug('Toggling ASR "Off"')
				await publish(Topics.ASR_TOGGLE_OFF, serialize({}))
			},
			onToggleOff: handler => {
				return on(Topics.ASR_TOGGLE_OFF, handler)
			},
			startListening: async (siteId, sessionId, startSignalMS) => {
				logger.debug('Start listening ASR for session "%s" on site "%s"', sessionId, siteId)
				await publish(Topics.ASR_START_LISTENING, serialize({
					siteId, sessionId, startSignalMS
				}))
			},
			onStartListening: handler => {
				return on(Topics.ASR_START_LISTENING, handler)
			},
			stopListening: async (siteId, sessionId) => {
				logger.debug('Stop listening ASR for session "%s" on site "%s"', sessionId || '<none>', siteId)
				await publish(Topics.ASR_STOP_LISTENING, serialize({
					siteId, sessionId
				}))
			},
			onStopListening: handler => {
				return on(Topics.ASR_STOP_LISTENING, handler)
			},
			textCaptured: async (siteId, sessionId, text, likelihood, seconds, tokens) => {
				logger.debug('ASR captured text "%s" for session "%s" on site "%s"', text, sessionId, siteId)
				await publish(Topics.ASR_TEXT_CAPTURED, serialize({
					siteId, sessionId, text, likelihood, seconds, tokens
				}))
			},
			onTextCaptured: handler => {
				return on(Topics.ASR_TEXT_CAPTURED, handler)
			},
			error: async (err, context) => {
				logger.error('ASR error:', err)
				await publish(Topics.ASR_ERROR, serialize({
					error: err,
					context
				}))
			},
			onError: handler => {
				return on(Topics.ASR_ERROR, handler)
			}
		},
		nlu: {
			load: async () => {
				logger.debug('NLU is loaded')
				await publish(Topics.NLU_LOAD, serialize({}))
			},
			onLoad: handler => {
				return on(Topics.NLU_LOAD, handler)
			},
			query: async (sessionId, input, intentFilter) => {
				id = UUID()
				logger.debug('Querying NLU with "%s" for session "%s"', input, sessionId)
				await publish(Topics.NLU_QUERY, serialize({
					sessionId, input, intentFilter, id
				}))
			},
			onQuery: handler => {
				return on(Topics.NLU_QUERY, handler)
			},
			intentParsed: async (sessionId, id, input, intentName, confidenceScore, slots) => {
				logger.debug('Request "%s" recognized intent "%s" from input "%s" for session "%s" with confidence %f', id, intentName, input, sessionId, confidenceScore)
				await publish(Topics.NLU_INTENT_PARSED, serialize({
					id, input, intent: {
						intentName, confidenceScore
					}, slots, sessionId
				}))
			},
			onIntentParsed: handler => {
				return on(Topics.NLU_INTENT_PARSED, handler)
			},
			intentNotRecognized: async (sessionId, id, input) => {
				logger.debug('Request "%s" did not recognized intent for input "%s" for session "%s"', id, input, sessionId)
				await publish(Topics.NLU_INTENT_NOT_RECOGNIZED, serialize({
					id, input, sessionId
				}))
			},
			onIntentNotRecognized: handler => {
				return on(Topics.NLU_INTENT_NOT_RECOGNIZED, handler)
			},
			error: async (err, context) => {
				logger.error('NLU error:', err)
				await publish(Topics.NLU_ERROR, serialize({
					error: err,
					context
				}))
			},
			onError: handler => {
				return on(Topics.NLU_ERROR, handler)
			}
		},
		tts: {
			load: async () => {
				logger.debug('TTS is loaded')
				await publish(Topics.TTS_LOAD, serialize({}))
			},
			onLoad: handler => {
				return on(Topics.TTS_LOAD, handler)
			},
			say: async (siteId, sessionId, text, lang, timeout) => {
				id = UUID()
				logger.debug('Speaking "%s" for session "%s" on site "%s"', text, sessionId, siteId)
				let p
				if ( timeout ) p = hermes.tts.waitForSayFinished(sessionId, id, timeout)
				publish(Topics.TTS_SAY, serialize({
					siteId, sessionId, id, text, lang
				}))
				await p
			},
			onSay: handler => {
				return on(Topics.TTS_SAY, handler)
			},
			sayFinished: async (sessionId, id) => {
				logger.debug('Speak request "%s" for session "%s" finished playing', id, sessionId)
				await publish(Topics.TTS_SAY_FINISHED, serialize({
					sessionId, id
				}))
			},
			onSayFinished: handler => {
				return on(Topics.TTS_SAY_FINISHED, handler)
			},
			waitForSayFinished: (sessionId, id, timeout) => {
				return waitFor(Topics.TTS_SAY_FINISHED, (topic, payload) => {
					if ( payload.id != id ) return
					logger.debug('Speaking "%s" for session "%s" finished', id, sessionId)
					return true
				}, timeout)
			},
			error: async (err, context) => {
				logger.error('TTS error:', err)
				await publish(Topics.TTS_ERROR, serialize({
					error: err,
					context
				}))
			},
			onError: handler => {
				return on(Topics.TTS_ERROR, handler)
			}
		},
		audioServer: {
			load: async (siteId) => {
				logger.debug('Audio server is loaded')
				await publish(Topics.AUDIO_SERVER_LOAD, serialize({siteId}))
			},
			onLoad: handler => {
				return on(Topics.AUDIO_SERVER_LOAD, handler)
			},
			audioFrame: async (siteId, chunk) => {
				logger.debug('Audio %d bytes frame on site "%s"', chunk.length, siteId)
				await publish(format(Topics.AUDIO_SERVER_AUDIO_FRAME, {siteId}), noop(chunk))
			},
			onAudioFrame: (siteId, handler) => {
				logger.debug('Listening for audio frames on site "%s"', siteId)
				return on(format(Topics.AUDIO_SERVER_AUDIO_FRAME, {siteId}), handler, noop)
			},
			playBytes: async (siteId, sessionId, bytes, timeout) => {
				id = UUID()
				logger.debug('Playing %d bytes for request "%s" on site "%s"', bytes.length, id, siteId)
				let p
				if ( timeout ) p = hermes.audioServer.waitForPlayFinished(siteId, id, timeout)
				await publish(format(Topics.AUDIO_SERVER_PLAY_BYTES, {siteId, id}), noop(bytes))
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
				return on(format(Topics.AUDIO_SERVER_PLAY_BYTES, {siteId, id: '+'}), wrapper, noop)
			},
			waitForPlayFinished: (siteId, id, timeout) => {
				logger.debug('Waiting for play "%s" finished on site "%s" for %d ms', id, siteId, timeout)
				return waitFor(format(Topics.AUDIO_SERVER_PLAY_FINISHED, {siteId, id}), (topic, payload) => {
					if ( payload.id != id ) return
					logger.debug('Playing "%s" finished on site "%s"', id, siteId)
					return true
				}, timeout)
			},
			playFinished: async (siteId, id) => {
				logger.debug('Playing "%s" finished on site "%s"', id, siteId)
				await publish(format(Topics.AUDIO_SERVER_PLAY_FINISHED, {siteId}), serialize({
					id, siteId
				}))
			},
			onPlayFinished: (siteId, handler) => {
				on(format(Topics.AUDIO_SERVER_PLAY_FINISHED, {siteId}), handler)
			},
			playBytesStream: async (siteId, sessionId, id, chunk, index, isLastChunk, timeout) => {
				id || (id = sessionId)
				index = '' + index
				isLastChunk = !!isLastChunk ? '1' : '0'
				// let p
				// if ( isLastChunk ) {
				// 	const p = hermes.audioServer.waitForStreamFinished(siteId, id, timeout)
				// }
				await publish(format(Topics.AUDIO_SERVER_PLAY_BYTES_STREAM, {siteId, id, index, isLastChunk}), chunk)
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
				return on(format(Topics.AUDIO_SERVER_PLAY_BYTES_STREAM, {siteId, id: '+', index: '+', isLastChunk: '+'}), wrapper, noop)
			},
			waitForStreamFinished: (siteId, id, timeout) => {
				logger.debug('Waiting for stream "%s" finished on site "%s" for %d ms', id, siteId, timeout)
				return waitFor(format(Topics.AUDIO_SERVER_STREAM_FINISHED, {siteId}), (topic, payload) => {
					if ( payload.id != id ) return
					logger.debug('Streaming "%s" finished on site "%s"', id, siteId)
					return true
				}, timeout)
			},
			streamFinished: async (siteId, id) => {
				logger.debug('Streaming "%s" finished on site "%s"', id, siteId)
				await publish(format(Topics.AUDIO_SERVER_STREAM_FINISHED, {siteId}), serialize({
					id, siteId
				}))
			},
			onStreamFinished: (siteId, handler) => {
				return on(format(Topics.AUDIO_SERVER_STREAM_FINISHED, {siteId}), handler)
			},
			replayRequest: async (id, siteId, startAtMS) => {
				requestId = id || UUID()
				logger.debug('Requesting replay "%s" from %d finished on site "%s"', requestId, startAtMS, siteId)
				await publish(Topics.AUDIO_SERVER_REPLAY_REQUEST, serialize({
					requestId, siteId, startAtMS
				}))
			},
			onReplayRequest: (handler) => {
				return on(Topics.AUDIO_SERVER_REPLAY_REQUEST, handler)
			},
			error: async (siteId, err, context) => {
				logger.error('Audio server error:', err)
				await publish(Topics.AUDIO_SERVER_ERROR, serialize({
					siteId,
					error: err,
					context
				}))
			},
			onError: handler => {
				return on(Topics.AUDIO_SERVER_ERROR, handler)
			}
		},
		injection: {
			types: InjectionTypes,
			perform: async (crossLanguage, lexicon, operations, timeout) => {
				id = UUID()
				logger.debug('Requesting injection "%s" with %d operations', id, operations.length)
				let p
				if ( timeout ) {
					p = waitForEither(
						Topics.INJECTION_COMPLETE,
						(topic, payload) => payload.requestId === id,
						Topics.INJECTION_FAILURE,
						(topic, payload) => payload.requestId === id,
						timeout
					)
				}
				await publish(Topics.INJECTION_PERFORM, serialize({
					id, crossLanguage, lexicon, operations
				}))
				await p
			},
			onPerform: handler => {
				return on(Topics.INJECTION_PERFORM, handler)
			},
			status: async (requestId, status, entity, injected) => {
				logger.debug('Injection "%s" status', requestId)
				await publish(Topics.INJECTION_STATUS, serialize({
					requestId, status, entity, injected
				}))
			},
			onStatus: (handler) => {
				return on(Topics.INJECTION_STATUS, handler)
			},
			complete: async (requestId) => {
				logger.debug('Injection "%s" complete', requestId)
				await publish(Topics.INJECTION_COMPLETE, serialize({
					requestId
				}))
			},
			onComplete: (handler) => {
				return on(Topics.INJECTION_COMPLETE, handler)
			},
			failure: async (requestId, err, context) => {
				logger.debug('Injection "%s" complete', requestId)
				await publish(Topics.INJECTION_FAILURE, serialize({
					requestId, error: err, context
				}))
			},
			onFailure: (handler) => {
				return on(Topics.INJECTION_FAILURE, handler)
			},
			waitForComplete: (requestId, timeout) => {
				logger.debug('Waiting for injection "%s" completed for %d ms', requestId, timeout)
				return waitFor(Topics.INJECTION_COMPLETE, (topic, payload) => {
					if ( payload.requestId != requestId ) return
					logger.debug('Injection "%s" complete', requestId)
					return true
				}, timeout)
			},
			reset: async (timeout) => {
				id = UUID()
				logger.debug('Requesting injection "%s" reset', id)
				let p
				if ( timeout ) p = hermes.injection.waitForResetComplete(id, timeout)
				await publish(Topics.INJECTION_RESET_PERFORM, serialize({
					id
				}))
				await p
			},
			onReset: handler => {
				return on(Topics.INJECTION_RESET_PERFORM, handler)
			},
			resetComplete: async (requestId) => {
				logger.debug('Injection "%s" reset', requestId)
				await publish(Topics.INJECTION_RESET_COMPLETE, serialize({
					requestId
				}))
			},
			onResetComplete: handler => {
				return on(Topics.INJECTION_RESET_COMPLETE, handler)
			},
			waitForResetComplete: (requestId, timeout) => {
				logger.debug('Waiting for injection "%s" completed for %d ms', requestId, timeout)
				return waitFor(Topics.INJECTION_RESET_COMPLETE, (topic, payload) => {
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
			if ( listeners.length === 0 ) {
				handlers.delete(topic)
				if ( client ) client.unsubscribe(topic)
				logger.debug('Unsubscribed from topic "%s"', topic)
				return
			}
			handlers.set(topic, {reg, listeners})
		}
		listeners.push(wrapper)
		handlers.set(topic, {reg, listeners})
		if ( client ) client.subscribe(topic)
		return wrapper
	}

	async function publish(topic, message, options) {
		if ( client && client.connected ) {
			logger.debug('Publishing topic "%s"', topic)
			await client.publish(topic, message, options)
		} else {
			queue.push({topic, message})
			if( queue.length > queueSize ) {
				logger.debug('Queue is full')
				queue.shift()
			}
			logger.debug('Queue has %d messages', queue.length)
		}
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
				reject(new Error('Wait timeout for topic "' + waitTopic + '"'))
			}, timeout || 30000)
		})
	}

	function waitForEither(successTopic, successHandler, failureTopic, failureHandler, timeout) {
		return new Promise((resolve, reject) => {
			logger.debug('Waiting for either "%s" or "%s" with timeout %d', successTopic, failureTopic, timeout)
			let _timeout, success, error
			const cleanup = () => {
				if ( _timeout ) clearTimeout(_timeout)
				if ( success ) success.remove()
				if ( error ) error.remove()
			}
			_timeout = setTimeout(() => {
				cleanup()
				reject(new Error('Timeout'))
			}, timeout)
			success = on(successTopic, (topic, payload) => {
				if ( !successHandler(topic, payload) ) return
				logger.debug('Received valid "%s"', successTopic)
				cleanup()
				resolve(payload)
			})
			error = on(failureTopic, (topic, payload) => {
				if ( !failureHandler(topic, payload) ) return
				logger.debug('Received valid "%s"', failureTopic)
				cleanup()
				reject(new Error(payload.error))
			})
		})
	}

	function createWav(bytes, options) {
		options = options || {}
		const RIFF				= Buffer.from('RIFF')
		const WAVE				= Buffer.from('WAVE')
		const fmt				= Buffer.from('fmt ')
		const data				= Buffer.from('data')

		const MAX_WAV			= 4294967295 - 100
		const endianness		= 'LE'
		const format			= 1 // raw PCM
		const channels			= options.channels || 1
		const sampleRate		= options.sampleRate || 44100
		const bitDepth			= options.bitDepth || 16

		const time				= options.time
		const replayId			= options.replayId
		const remainingFrames	= options.remainingFrames

		let headerLength = 44
		if ( time !== undefined ) {
			headerLength += 4 + 4 + 8
		}
		if ( replayId !== undefined ) {
			headerLength += 4 + 4 + replayId.length
		}
		if ( remainingFrames !== undefined ) {
			headerLength += 4 + 4 + 4
		}

		const dataLength	= bytes.length || MAX_WAV
		const fileSize		= dataLength + headerLength
		const header		= Buffer.alloc(headerLength)

		let offset			= 0

		// write the "RIFF" identifier
		RIFF.copy(header, offset)
		offset += RIFF.length

		// write the file size minus the identifier and this 32-bit int
		header['writeUInt32' + endianness](fileSize - 8, offset)
		offset += 4

		// write the "WAVE" identifier
		WAVE.copy(header, offset)
		offset += WAVE.length

		// write the "fmt " sub-chunk identifier
		fmt.copy(header, offset)
		offset += fmt.length

		// write the size of the "fmt " chunk
		// XXX: value of 16 is hard-coded for raw PCM format. other formats have
		// different size.
		header['writeUInt32' + endianness](16, offset)
		offset += 4

		// write the audio format code
		header['writeUInt16' + endianness](format, offset)
		offset += 2

		// write the number of channels
		header['writeUInt16' + endianness](channels, offset)
		offset += 2

		// write the sample rate
		header['writeUInt32' + endianness](sampleRate, offset)
		offset += 4

		// write the byte rate
		var byteRate = sampleRate * channels * bitDepth / 8
		header['writeUInt32' + endianness](byteRate, offset)
		offset += 4

		// write the block align
		var blockAlign = channels * bitDepth / 8
		header['writeUInt16' + endianness](blockAlign, offset)
		offset += 2

		// write the bits per sample
		header['writeUInt16' + endianness](bitDepth, offset)
		offset += 2

		// write the "time" metadata
		if ( time !== undefined ) {
			const time = Buffer.from('time')
			time.copy(header, offset)
			offset += time.length

			// write the timestamp as U64LE
			header['writeUInt32' + endianness](8, offset)
			offset += 4
			header['writeBigUInt64' + endianness](BigInt(options.time), offset)
			offset += 8
		}

		// write the "rpid" metadata
		if ( replayId !== undefined ) {
			const rpid = Buffer.from('rpid')
			rpid.copy(header, offset)
			offset += rpid.length

			header['writeUInt32' + endianness](replayId.length, offset)
			offset += 4
			header['write'](replayId, offset)
			offset += replayId.length
		}

		// write the "rprf" metadata
		if ( remainingFrames !== undefined ) {
			const rprf = Buffer.from('rprf')
			rprf.copy(header, offset)
			offset += rprf.length

			header['writeUInt32' + endianness](4, offset)
			offset += 4
			header['writeUInt32' + endianness](options.remainingFrames, offset)
			offset += 4
		}

		// write the "data" sub-chunk ID
		data.copy(header, offset)
		offset += data.length

		// write the remaining length of the rest of the data
		header['writeUInt32' + endianness](dataLength, offset)
		offset += 4

		// flush the header and after that pass-through "dataLength" bytes
		return Buffer.concat([header, bytes])
	}

	function readWav(bytes) {

		let offset = 0

		const endianness = 'LE'

		const RIFF = bytes.slice(0, 4)
		offset += RIFF.length

		// read the file size minus the identifier and this 32-bit int
		const filesize = bytes['readUInt32' + endianness](offset) + 8
		offset += 4

		// read the "WAVE" identifier
		const WAVE = bytes.slice(offset, offset + 4)
		offset += WAVE.length

		const wav = {
			endianness
		}

		while ( offset < bytes.length ) {
			const chunk = bytes.slice(offset, offset + 4)
			offset += chunk.length
			switch ( chunk.toString() ) {
				case 'fmt ':
					// read the size of the "fmt " chunk
					// XXX: value of 16 is hard-coded for raw PCM format. other formats have
					// different size.
					wav.fmtSize = bytes['readUInt32' + endianness](offset)
					offset += 4

					// read the audio format code
					wav.format = bytes['readUInt16' + endianness](offset)
					offset += 2

					// read the number of channels
					wav.channels = bytes['readUInt16' + endianness](offset)
					offset += 2

					// read the sample rate
					wav.sampleRate = bytes['readUInt32' + endianness](offset)
					offset += 4

					// read the byte rate
					wav.byteRate = bytes['readUInt32' + endianness](offset)
					offset += 4

					// read the block align
					wav.blockAlign = bytes['readUInt16' + endianness](offset)
					offset += 2

					// read the bits per sample
					wav.bitDepth = bytes['readUInt16' + endianness](offset)
					offset += 2
					break

				case 'time':
					// read the timestamp as U64LE
					var timestampLength = bytes['readUInt32' + endianness](offset)
					offset += 4

					wav.time = Number(bytes['readBigUInt64' + endianness](offset))
					offset += timestampLength
					break

				case 'rpid':
					var replayIdLength = bytes['readUInt32' + endianness](offset)
					offset += 4

					wav.replayId = bytes.slice(offset, offset + replayIdLength).toString()
					offset += replayIdLength
					break

				case 'rprf':
					// read the "rprf" metadata
					var remainingFramesLength = bytes['readUInt32' + endianness](offset)
					offset += 4

					wav.remainingFrames = bytes['readUInt32' + endianness](offset)
					offset += remainingFramesLength
					break

				case 'data':
					// read the remaining length of the rest of the data
					var sampleLength = bytes['readUInt32' + endianness](offset)
					offset += 4

					wav.data = bytes.slice(offset, offset + sampleLength)
					offset += sampleLength
					break
			}
		}

		return wav
	}

	return hermes
}