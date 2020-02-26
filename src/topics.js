module.exports = {
	DIALOGUE_LOAD 						: 'hermes/dialogueManager/load',
	DIALOGUE_START_SESSION				: 'hermes/dialogueManager/startSession',
	DIALOGUE_SESSION_STARTED			: 'hermes/dialogueManager/sessionStarted',
	DIALOGUE_SESSION_QUEUED				: 'hermes/dialogueManager/sessionQueued',
	DIALOGUE_CONTINUE_SESSION			: 'hermes/dialogueManager/continueSession',
	DIALOGUE_END_SESSION				: 'hermes/dialogueManager/endSession',
	DIALOGUE_SESSION_ENDED				: 'hermes/dialogueManager/sessionEnded',
	DIALOGUE_INTENT_NOT_RECOGNIZED		: 'hermes/dialogueManager/intentNotRecognized',
	DIALOGUE_ERROR 						: 'hermes/dialogueManager/error',

	FEEDBACK_SOUND_TOGGLE_ON			: 'hermes/feedback/sound/toggleOn',
	FEEDBACK_SOUND_TOGGLE_OFF			: 'hermes/feedback/sound/toggleOff',

	DIALOGUE_INTENT 					: 'hermes/intent/{intentName}',

	HOTWORD_LOAD 						: 'hermes/hotword/load',
	HOTWORD_TOGGLE_ON					: 'hermes/hotword/toggleOn',
	HOTWORD_TOGGLE_OFF					: 'hermes/hotword/toggleOff',
	HOTWORD_DETECTED					: 'hermes/hotword/{modelId}/detected',
	HOTWORD_ERROR 						: 'hermes/hotword/error',

	ASR_LOAD 							: 'hermes/asr/load',
	ASR_TOGGLE_ON						: 'hermes/asr/toggleOn',
	ASR_TOGGLE_OFF						: 'hermes/asr/toggleOff',
	ASR_START_LISTENING					: 'hermes/asr/startListening',
	ASR_STOP_LISTENING					: 'hermes/asr/stopListening',
	ASR_TEXT_CAPTURED					: 'hermes/asr/textCaptured',
	ASR_ERROR							: 'hermes/asr/error',

	NLU_LOAD 							: 'hermes/nlu/load',
	NLU_QUERY							: 'hermes/nlu/query',
	NLU_INTENT_PARSED					: 'hermes/nlu/intentParsed',
	NLU_INTENT_NOT_RECOGNIZED			: 'hermes/nlu/intentNotRecognized',
	NLU_ERROR							: 'hermes/nlu/error',

	TTS_LOAD 							: 'hermes/tts/load',
	TTS_SAY								: 'hermes/tts/say',
	TTS_SAY_FINISHED					: 'hermes/tts/sayFinished',
	TTS_ERROR							: 'hermes/tts/error',

	AUDIO_SERVER_LOAD 					: 'hermes/audioServer/load',
	AUDIO_SERVER_AUDIO_FRAME			: 'hermes/audioServer/{siteId}/audioFrame',
	AUDIO_SERVER_PLAY_BYTES				: 'hermes/audioServer/{siteId}/playBytes/{id}',
	AUDIO_SERVER_PLAY_FINISHED			: 'hermes/audioServer/{siteId}/playFinished',
	AUDIO_SERVER_PLAY_BYTES_STREAM		: 'hermes/audioServer/{siteId}/playBytesStreaming/{id}/{index}/{isLastChunk}',
	AUDIO_SERVER_STREAM_FINISHED		: 'hermes/audioServer/{siteId}/streamFinished',
	AUDIO_SERVER_ERROR 					: 'hermes/audioServer/error'
}