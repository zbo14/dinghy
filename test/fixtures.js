'use strict'

const {it} = require('mocha')
const {expect} = require('chai')
const Server = require('../lib')

const address = 'localhost'
const numServers = 5

const info = []
const servers = []

const iter = (f, start = 0, end = numServers) => {
  for (let id = start; id < end; id++) {
    f(id)
  }
}

exports.initServers = port => {
  it('initializes servers', () => {
    iter(id => {
      info[id] = {
        id,
        port: port + id,
        address
      }
    })
    iter(id1 => {
      servers[id1] = new Server(info[id1])
      iter(id2 => {
        if (id1 !== id2) servers[id1].addPeer(info[id2])
      })
    })
  })
}

exports.startServers = () => {
  it('starts servers', () => {
    servers.forEach(server => server.start())
  })
}

exports.startServersWithLeader = () => {
  it('start servers with a leader', () => {
    servers[0].startAsLeader()
    iter(id => servers[id].start(), 1)
  })
}

exports.stopServers = () => {
  it('stops servers', () => {
    servers.forEach(server => server.stop())
  })
}

exports.expectLeaderCommit = commitIndex => {
  it('checks leader\'s commit index', () => {
    expect(servers[0].commitIndex).to.equal(commitIndex)
  })
}

exports.expectOneLeader = () => {
  it('checks there is one leader', () => {
    const leaders = servers.filter(server => server.isLeader())
    expect(leaders).to.have.lengthOf(1)
  })
}

exports.expectCurrentTerm = term => {
  it('checks current term term', () => {
    servers.forEach(({currentTerm}) => expect(currentTerm).to.equal(term))
  })
}

exports.sendClientCommand = () => {
  it('mocks client sending command to leader', () => {
    servers[0].command(0)
  })
}

exports.wait = timeout => {
  it('waits for a bit', done => {
    setTimeout(done, timeout)
  })
}
