'use strict'

const {
  expectLeaderCommit,
  expectOneLeader,
  initServers,
  sendClientCommand,
  startServersWithLeader,
  stopServers,
  wait
} = require('./fixtures')

module.exports = () => {
  initServers(5000)
  startServersWithLeader()
  expectOneLeader()
  expectLeaderCommit(-1)
  sendClientCommand()
  wait(300)
  expectOneLeader()
  expectLeaderCommit(0)
  sendClientCommand()
  wait(300)
  expectOneLeader()
  expectLeaderCommit(1)
  stopServers()
}
