'use strict'

const {createSocket} = require('dgram')

/**
 * @typedef {Object} Entry
 * @property {number} term - the term for this entry.
 * @property {number} command - the command sent by the client.
 */

/**
 * @typedef {Object} Info
 * @property {number} id - the server id.
 * @property {number} port - the port the server is listening on.
 * @property {string} address - the server's address.
 * @property {number} [nextIndex] - the index of the next log entry to send to the server.
 * @property {number} [matchIndex] - the index of the highest log entry know to be replicated on the server.
 */

/**
 * @typedef {Object} Message
 * @property {string} type - the type of message.
 * @property {number} id - the id of the message sender.
 * @property {number} term - the current term of the message sender.
 * @property {object} data - the data specific to the type of message.
 */

/**
 * Represents a server.
 *
 * @constructor
 * @param {Info} - the server info.
 */
function Server (info) {
  this.init(info)
}

// Init

Server.prototype.init = function (info) {
  this.commitIndex = -1
  this.currentTerm = -1
  this.electionTimeout = null
  this.heartbeat = null
  this.info = Object.assign({}, info)
  this.lastApplied = -1
  this.log = []
  this.numVotes = 0
  this.peers = []
  this.socket = createSocket('udp4')
  this.socket.on('message', msg => this.handleMessage(msg))
  this.socket.on('error', err => this.handleError(err))
  this.socket.bind(info.port, info.address)
  this.votedFor = null
}

Server.prototype.command = function (command) {
  if (!this.isLeader()) return this.handleUnexpectedRole('leader')
  this.log.push({term: this.currentTerm, command})
}

// Election timeout

Server.prototype.clearElectionTimeout = function () {
  clearTimeout(this.electionTimeout)
}

Server.prototype.startElectionTimeout = function () {
  const timeout = 150 + Math.round(Math.random() * 150)
  this.electionTimeout = setTimeout(() => this.startElection(), timeout)
}

// Heartbeat timeout

Server.prototype.clearHeartbeatTimeout = function () {
  clearTimeout(this.heartbeatTimeout)
}

Server.prototype.startHeartbeatTimeout = function () {
  this.heartbeatTimeout = setTimeout(() => this.startElectionTimeout(), 300)
}

// Heartbeat

Server.prototype.clearHeartbeat = function () {
  clearInterval(this.heartbeat)
}

Server.prototype.startHeartbeat = function () {
  this.heartbeat = setInterval(() => this.broadcastAppendEntries(), 50)
}

// Iterators

Server.prototype.forEachPeer = function (cb) {
  this.peers.forEach(cb)
}

// Send

Server.prototype.sendAppendEntries = function (id) {
  const {nextIndex} = this.peer(id)
  const prevLogIndex = nextIndex - 1
  this.send(id, {
    type: 'appendEntries',
    id: this.id(),
    term: this.currentTerm,
    data: {
      commitIndex: this.commitIndex,
      prevLogIndex,
      prevLogTerm: this.entryTerm(prevLogIndex),
      entries: this.log.slice(nextIndex)
    }
  })
}

Server.prototype.sendRespondEntries = function (id, success, numEntries = null) {
  this.send(id, {
    type: 'respondEntries',
    id: this.id(),
    term: this.currentTerm,
    data: {success, numEntries}
  })
}

Server.prototype.sendRespondVote = function (id, voteGranted) {
  this.send(id, {
    type: 'respondVote',
    id: this.id(),
    term: this.currentTerm,
    data: {voteGranted}
  })
}

Server.prototype.send = function (id, msg) {
  const {port, address} = this.peer(id)
  this.socket.send(JSON.stringify(msg), port, address)
}

// Broadcast

Server.prototype.broadcastAppendEntries = function (entries) {
  const lastLogIndex = this.lastLogIndex()
  this.forEachPeer(({id, nextIndex}) => {
    if (lastLogIndex >= 0 && lastLogIndex < nextIndex) return
    this.sendAppendEntries(id)
  })
}

Server.prototype.broadcastRequestVote = function () {
  this.broadcast({
    type: 'requestVote',
    id: this.id(),
    term: this.currentTerm,
    data: {
      lastLogIndex: this.lastLogIndex(),
      lastLogTerm: this.lastLogTerm()
    }
  })
}

Server.prototype.broadcast = function (msg) {
  this.forEachPeer(({id}) => this.send(id, msg))
}

// Handlers

Server.prototype.handleError = function (err) {
  throw err
}

Server.prototype.handleAppendEntries = function ({id, term, data: {commitIndex, entries, prevLogIndex, prevLogTerm}}) {
  this.clearElectionTimeout()
  this.clearHeartbeatTimeout()
  if (this.isLeader()) return this.handleUnexpectedRole('follower or candidate')
  if (this.isCandidate()) {
    this.becomeFollower()
    this.clearVote()
  }
  this.startHeartbeatTimeout()
  if (
    this.behindCurrentTerm(term) ||
    this.entryMismatch(prevLogIndex, prevLogTerm)
  ) return this.sendRespondEntries(id, false)
  this.deleteEntries(prevLogIndex + 1)
  this.appendEntries(entries)
  this.updateCommitIndex(commitIndex)
  this.sendRespondEntries(id, true, entries.length)
}

Server.prototype.handleRespondEntries = function ({id, term, data: {success, numEntries}}) {
  if (!this.isLeader()) return
  if (success) {
    this.updatePeer(id, numEntries)
    return this.advanceCommit()
  }
  if (this.aheadCurrentTerm(term)) {
    this.clearHeartbeat()
    this.becomeFollower()
    this.clearVote()
    this.startElectionTimeout()
    return
  }
  this.decrementNextIndex(id)
  this.sendAppendEntries(id)
}

Server.prototype.handleRequestVote = function ({id, term, data: {lastLogIndex, lastLogTerm}}) {
  if (
    this.votedForAnotherCandidate(id) ||
    this.behindCurrentTerm(term) ||
    !this.logUpToDate(lastLogIndex, lastLogTerm)
  ) return this.sendRespondVote(id, false)
  this.clearElectionTimeout()
  this.sendRespondVote(id, true)
  this.voteFor(id)
}

Server.prototype.handleRespondVote = function ({id, term, data: {voteGranted}}) {
  if (!this.isCandidate()) return
  if (!voteGranted) {
    if (this.aheadCurrentTerm(term)) {
      this.clearElectionTimeout()
      this.becomeFollower()
      this.clearVote()
      this.startElectionTimeout()
    }
    return
  }
  console.log('got vote')
  if (this.hasMajorityVotes()) {
    this.clearElectionTimeout()
    this.startLeadership()
  }
}

Server.prototype.handleUnexpectedMessageType = function (type) {
  this.handleError(new Error(`unexpected message type: ${type}`))
}

Server.prototype.handleUnexpectedRole = function (role) {
  this.handleError(new Error(`expected role ${role}, got ${this.role}`))
}

Server.prototype.handleMessage = function (msg) {
  try {
    msg = JSON.parse(msg.toString())
    switch (msg.type) {
      case 'appendEntries':
        return this.handleAppendEntries(msg)
      case 'requestVote':
        return this.handleRequestVote(msg)
      case 'respondEntries':
        return this.handleRespondEntries(msg)
      case 'respondVote':
        return this.handleRespondVote(msg)
      default:
        this.handleUnexpectedMessageType(msg.type)
    }
  } catch (err) {
    this.handleError(err)
  }
}

// Accessors

Server.prototype.addPeer = function (info) {
  const peer = Object.assign({}, info)
  peer.matchIndex = null
  peer.nextIndex = null
  this.peers.push(peer)
}

Server.prototype.advanceCommit = function () {
  const nextCommit = this.commitIndex + 1
  const entry = this.log[nextCommit]
  if (entry == null || entry.term !== this.currentTerm) return
  let count = 0
  this.forEachPeer(({matchIndex}) => {
    if (matchIndex >= nextCommit) count++
  })
  if (count < this.majority()) return
  console.log('advanceCommit')
  this.commitIndex = nextCommit
  this.advanceCommit()
}

Server.prototype.aheadCurrentTerm = function (term) {
  if (term <= this.currentTerm) return false
  this.setCurrentTerm(term)
  return true
}

Server.prototype.appendEntries = function (...entries) {
  this.log.push(...entries)
}

Server.prototype.becomeCandidate = function () {
  if (!this.isFollower()) return this.handleUnexpectedRole('follower')
  this.role = 'candidate'
  console.log(`${this.id()} became candidate`)
}

Server.prototype.becomeFollower = function () {
  this.role = 'follower'
}

Server.prototype.becomeLeader = function () {
  if (!this.isCandidate()) return this.handleUnexpectedRole('candidate')
  this.role = 'leader'
  console.log(`${this.id()} became leader`)
}

Server.prototype.behindCurrentTerm = function (term) {
  if (this.currentTerm > term) return true
  else if (this.currentTerm < term) this.setCurrentTerm(term)
  return false
}

Server.prototype.clearVote = function () {
  this.voteFor(null)
}

Server.prototype.decrementNextIndex = function (id) {
  this.peer(id).nextIndex--
}

Server.prototype.deleteEntries = function (nextIndex) {
  this.log.splice(nextIndex, this.log.length)
}

Server.prototype.entryMismatch = function (prevLogIndex, prevLogTerm) {
  if (prevLogIndex < 0) return false
  const entry = this.log[prevLogIndex]
  return entry == null || entry.term !== prevLogTerm
}

Server.prototype.entryTerm = function (index) {
  const entry = this.log[index]
  return entry ? entry.term : null
}

Server.prototype.hasMajorityVotes = function () {
  return ++this.numVotes >= this.majority()
}

Server.prototype.hasRole = function (role) {
  return this.role === role
}

Server.prototype.id = function () {
  return this.info.id
}

Server.prototype.incrementCurrentTerm = function () {
  this.currentTerm += 1
}

Server.prototype.isCandidate = function () {
  return this.hasRole('candidate')
}

Server.prototype.isFollower = function () {
  return this.hasRole('follower')
}

Server.prototype.isLeader = function () {
  return this.hasRole('leader')
}

Server.prototype.lastLogIndex = function () {
  return this.log.length - 1
}

Server.prototype.lastLogTerm = function () {
  return this.entryTerm(this.lastLogIndex())
}

Server.prototype.logUpToDate = function (lastLogIndex, lastLogTerm) {
  if (this.lastLogTerm() > lastLogTerm) return false
  if (this.lastLogTerm() < lastLogTerm) return true
  return this.lastLogIndex() <= lastLogIndex
}

Server.prototype.majority = function () {
  return Math.ceil(this.numServers() / 2)
}

Server.prototype.numServers = function () {
  return this.peers.length + 1
}

Server.prototype.peer = function (id) {
  return this.peers.find(peer => id === peer.id)
}

Server.prototype.setCurrentTerm = function (term) {
  this.currentTerm = term
}

Server.prototype.updateCommitIndex = function (commitIndex) {
  if (commitIndex <= this.commitIndex) return
  this.commitIndex = Math.min(commitIndex, this.lastLogIndex())
}

// see https://groups.google.com/forum/#!topic/raft-dev/vCJcFi769x4
Server.prototype.updatePeer = function (id, numEntries) {
  const peer = this.peer(id)
  peer.matchIndex = peer.nextIndex + numEntries - 1
  peer.nextIndex = peer.matchIndex + 1
}

Server.prototype.votedForAnotherCandidate = function (id) {
  return this.votedFor != null && this.votedFor !== id
}

Server.prototype.voteFor = function (id) {
  this.votedFor = id
}

Server.prototype.voteForSelf = function () {
  this.numVotes = 1
  this.voteFor(this.id())
}

// Actions

Server.prototype.startElection = function () {
  this.becomeCandidate()
  this.incrementCurrentTerm()
  this.voteForSelf()
  this.broadcastRequestVote()
  this.startElectionTimeout()
}

Server.prototype.startLeadership = function () {
  this.becomeLeader()
  this.initPeers()
  this.startHeartbeat()
}

Server.prototype.initPeers = function () {
  const nextIndex = this.lastLogIndex() + 1
  this.forEachPeer(peer => {
    peer.matchIndex = -1
    peer.nextIndex = nextIndex
  })
}

// Start

Server.prototype.start = function () {
  this.becomeFollower()
  this.clearVote()
  this.startElectionTimeout()
}

// for testing
Server.prototype.startAsLeader = function () {
  this.becomeFollower()
  this.becomeCandidate()
  this.startLeadership()
}

// Stop

Server.prototype.stop = function () {
  this.clearElectionTimeout()
  this.clearHeartbeat()
}

module.exports = Server
